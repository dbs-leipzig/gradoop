/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Graph;
import org.gradoop.flink.io.impl.csv.pojos.Key;
import org.gradoop.flink.io.impl.csv.pojos.Properties;
import org.gradoop.flink.io.impl.csv.pojos.Ref;
import org.gradoop.flink.io.impl.csv.pojos.Reference;
import org.gradoop.flink.io.impl.csv.pojos.Static;
import org.gradoop.flink.io.impl.csv.pojos.Staticorreference;
import org.gradoop.flink.io.impl.csv.pojos.Vertexedge;
import org.gradoop.flink.io.impl.csv.pojos.Label;
import org.gradoop.flink.io.impl.csv.pojos.Objectreferences;
import org.gradoop.flink.io.impl.csv.pojos.Property;
import org.gradoop.flink.io.impl.csv.tuples.ReferenceTuple;

import java.util.List;
import java.util.regex.Pattern;


/**
 * Creates EPGMElements from a csv object which contains the meta information
 * to the corresponding content.
 */
public class CSVToElement implements
  FlatMapFunction<Tuple2<Csv, List<String>>, EPGMElement> {
  /**
   * EPGMElement which will be initialized as the specific element defined in
   * the csv object.
   */
  private EPGMElement reuse;
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  private EdgeFactory edgeFactory;

  /**
   * Creates map function.
   *
   * @param graphHeadFactory EPGM graph head factory
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   */
  public CSVToElement(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(Tuple2<Csv, List<String>> tuple,
    Collector<EPGMElement> collector) throws Exception {

    Csv csv = tuple.f0;
    List<String> content = tuple.f1;
    //one EPGMElement for each line
    for (String line : content) {
      String[] fields = line.split(Pattern.quote(csv.getSeparator()));
      //one csv element contains only one type of element
      if (csv.getGraphhead() != null) {
        reuse = createGraphHead(csv, fields);
      } else if (csv.getVertex() != null) {
        reuse = createVertex(csv, fields);
        //if the vertex also defines an outgoing edge is is also collected
        if (csv.getVertex().getEdges() != null) {
          for (Vertexedge vertexEdge : csv.getVertex().getEdges()
            .getVertexedge()) {
            collector.collect(
              createEdge(csv, fields, vertexEdge, reuse.getPropertyValue(
                CSVConstants.PROPERTY_KEY_KEY).getString()));
          }
        }
      } else if (csv.getEdge() != null) {
        reuse = createEdge(csv, fields);
      } else {
        reuse = null;
      }
      collector.collect(reuse);
    }
  }

  /**
   * Creates a graph head from the given fields by using the csv meta
   * information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return GraphHead
   */
  private org.gradoop.common.model.impl.pojo.GraphHead createGraphHead(Csv csv,
  String[] fields) {
    String label = "";
    if (csv.getGraphhead().getLabel() != null) {
      label = createLabel(csv.getGraphhead().getLabel(), fields);
    }
    Key key = csv.getGraphhead().getKey();
    List<Properties> propertiesCsv = csv.getGraphhead().getProperties();
    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);
    return graphHeadFactory.createGraphHead(label, properties);
  }

  /**
   * Creates a vertex from the given fields by using the csv meta
   * information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return Vertex
   */
  private org.gradoop.common.model.impl.pojo.Vertex createVertex(Csv csv,
    String[] fields) {
    String label = "";
    if (csv.getVertex().getLabel() != null) {
      label = createLabel(csv.getVertex().getLabel(), fields);
    }
    Key key = csv.getVertex().getKey();
    List<Properties> propertiesCsv = csv.getVertex().getProperties();
    String className = csv.getVertex().getKey().getClazz();
    List<Graph> graphs = csv.getVertex().getGraphs().getGraph();

    String graphList = createGraphList(graphs, csv.getDatasourceName(), csv
      .getDomainName(), className, fields);

    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);
    properties.set(CSVConstants.PROPERTY_KEY_GRAPHS, graphList);

    return vertexFactory.createVertex(label, properties);
  }

  /**
   * Creates an edge from the given fields by using the csv meta
   * information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return Edge
   */
  private org.gradoop.common.model.impl.pojo.Edge createEdge(Csv csv,
    String[] fields) {
    //'null' and "" to create a normal edge and not one initialized by a vertex
    return createEdge(csv, fields, null, "");
  }

  /**
   * Creates an edge from the given fields by using the csv meta
   * information. May be called to create an edge directly from an vertex.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @param edge vertex edge defined inside the vertex field of the csv
   * @param sourceKey the concatenated key of the source vertex
   * @return Edge
   */
  private org.gradoop.common.model.impl.pojo.Edge createEdge(Csv csv,
    String[] fields, Vertexedge edge, String sourceKey) {
    String label = "";

    if (edge == null) {
      edge = csv.getEdge();
    }
    if (edge.getLabel() != null) {
      label = createLabel(edge.getLabel(), fields);
    }
    Key key = edge.getKey();
    List<Properties> propertiesCsv = edge.getProperties();
    String className = edge.getKey().getClazz();
    List<Graph> graphs = edge.getGraphs().getGraph();

    String graphList = createGraphList(graphs, csv.getDatasourceName(), csv
      .getDomainName(), className, fields);

    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);
    properties.set(CSVConstants.PROPERTY_KEY_GRAPHS, graphList);

    ReferenceTuple referenceTuple;
    // normal edge, so information can be read by the csv-edge meta information
    if (sourceKey.equals("")) {
      //relevant key information for the source
      referenceTuple = this.setNamesAndIds(csv.getEdge().getSource(), fields,
        csv.getDatasourceName(), csv.getDomainName(), className);
      sourceKey = createKey(referenceTuple);
    }
    //relevant key information for the target
    referenceTuple = this.setNamesAndIds(
      edge.getTarget(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);
    String targetKey = createKey(referenceTuple);

    properties.set(CSVConstants.PROPERTY_KEY_SOURCE, sourceKey);
    properties.set(CSVConstants.PROPERTY_KEY_TARGET, targetKey);

    return edgeFactory.createEdge(
      label, GradoopId.get(), GradoopId.get(), properties);
  }

  /**
   * Creates a key which is defined through all parameter below.
   *
   * @param graphs graphlist
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @param fields contains the data
   * @return concatenated string representing the key
   */
  private String createGraphList(List<Graph> graphs, String datasourceName,
    String domainName, String className, String[] fields) {
    StringBuilder sb = new StringBuilder();
    boolean notFirst = false;
    for (Graph graph : graphs) {
      if (!notFirst) {
        notFirst = true;
      } else {
        sb.append(CSVConstants.SEPARATOR_GRAPHS);
      }
      sb.append(createKey(this.setNamesAndIds(
        graph, fields, datasourceName, domainName, className))
        .replaceAll(CSVConstants.SEPARATOR_GRAPHS,
          CSVConstants.ESCAPE_REPLACEMENT_GRAPHS));
    }
    return sb.toString();
  }

  /**
   * Creates a Tuple which contains the datasource name, the domain name, the
   * class name and the id.
   *
   * @param staticOrReference contains the id information
   * @param fields contains the data
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @return tuple containing all relevant information
   */
  private ReferenceTuple setNamesAndIds(Staticorreference staticOrReference,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    //start the id with the static name, if it is set
    tuple.setId((staticOrReference.getStatic() != null) ?
      staticOrReference.getStatic().getName() : "");
    //extends id by all class internal references
    boolean refSet = false;
    for (Ref ref : staticOrReference.getRef()) {
      tuple.setId(tuple.getId() + fields[ref.getColumnId().intValue()]);
      refSet = true;
    }
    //extends id by any external reference
    for (Reference reference : staticOrReference.getReference()) {
      if (reference.getDatasourceName() != null) {
        tuple.setDatasourceName(reference.getDatasourceName());
      }
      if (reference.getDomainName() != null) {
        tuple.setDomainName(reference.getDomainName());
      }
      if (refSet) {
        tuple
          .setClassName(tuple.getClassName() + reference.getKey().getClazz());
      } else {
        tuple.setClassName(reference.getKey().getClazz());
      }
      tuple
        .setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference
          .getKey().getContent(), fields, ""));
    }
    return tuple;
  }

  /**
   * Creates a Tuple which contains the datasource name, the domain name, the
   * class name and the id.
   *
   * @param objectReferences contains objects with the id information
   * @param fields contains the data
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @return tuple containing all relevant information
   */
  private ReferenceTuple setNamesAndIds(Objectreferences objectReferences,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId("");
    //contains only object, but information extraction depends on their class
    boolean refSet = false;
    for (Object object : objectReferences.getStaticOrRefOrReference()) {
      if (Static.class.isInstance(object)) {
        tuple.setId(((Static) object).getName());
      } else  if (Ref.class.isInstance(object)) {
        tuple.setId(
          tuple.getId() + fields[((Ref) object).getColumnId().intValue()]);
        refSet = true;
      } else if (Reference.class.isInstance(object)) {
        Reference reference = (Reference) object;
        if (reference.getDatasourceName() != null) {
          tuple.setDatasourceName(reference.getDatasourceName());
        }
        if (reference.getDomainName() != null) {
          tuple.setDomainName(reference.getDomainName());
        }
        if (refSet) {
          tuple
            .setClassName(tuple.getClassName() + reference.getKey().getClazz());
        } else {
          tuple.setClassName(reference.getKey().getClazz());
        }
        tuple
          .setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference
            .getKey().getContent(), fields, ""));
      }
    }
    return tuple;
  }

  /**
   * Creates a string containing all entries form a Static of a Ref object which
   * are separated by a given string.
   *
   * @param objects list of objects which are either from class Static or Ref
   * @param fields contains the data
   * @param separator separates each entry for the string
   * @return String of all separated entries
   */
  private String getEntriesFromStaticOrRef(List<Object> objects, String[]
    fields, String separator) {
    String contentString = "";
    for (Object object : objects) {
      if (Static.class.isInstance(object)) {
        contentString = ((Static) object).getName();
      } else if (Ref.class.isInstance(object)) {
        if (!contentString.equals("") && !separator.equals("")) {
          contentString += separator;
        }
        if (separator.equals(CSVConstants.SEPARATOR_LABEL)) {
          contentString += fields[((Ref) object).getColumnId().intValue()]
            .replaceAll(CSVConstants.SEPARATOR_LABEL,
              CSVConstants.ESCAPE_REPLACEMENT_LABEL);
        } else {
          contentString += fields[((Ref) object).getColumnId().intValue()];
        }
      }
    }
    return contentString;
  }

  /**
   * Creates a label from the given meta information and the data.
   *
   * @param label meta information
   * @param fields contains the data
   * @return string representation of the label
   */
  private String createLabel(Label label, String[] fields) {
    String labelSeparator = (label.getSeparator() == null) ?
      CSVConstants.SEPARATOR_LABEL : label.getSeparator();
    return getEntriesFromStaticOrRef(label.getContent(),
      fields, labelSeparator);
  }

  /**
   * Creates a string representation of the key which is defined by the
   * datasource name, the domain name, the class name and the id.
   *
   * @param tuple contains all relevant information
   * @return string representation of the key
   */
  private String createKey(ReferenceTuple tuple) {
    return tuple.getDatasourceName().replaceAll(CSVConstants.SEPARATOR_KEY,
      CSVConstants.ESCAPE_REPLACEMENT_KEY) + "_" +
      tuple.getDomainName().replaceAll(
        CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY) + "_" +
      tuple.getClassName().replaceAll(
        CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY) + "_" +
      tuple.getId().replaceAll(
        CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY);
  }

  /**
   * Creates the properties used for an epgm representation of an element.
   *
   * @param csv contains the meta information
   * @param properties list of properties from the csv
   * @param key the key from the csv for the current object
   * @param fields contains the data
   * @return epgm property list
   */
  private PropertyList createProperties(Csv csv, List<Properties> properties,
    Key key, String[] fields) {
    PropertyList list = PropertyList.create();
    String resultKey = createKey(
      new ReferenceTuple(csv.getDatasourceName(), csv.getDomainName(),
        key.getClazz(), getEntriesFromStaticOrRef(key.getContent(),
          fields, "")));

    PropertyValue value = new PropertyValue();
    value.setString(resultKey);

    list.set(CSVConstants.PROPERTY_KEY_KEY, value);

    //load all properties and set their type according to the type specified
    //in the meta information
    if (properties != null && !properties.isEmpty()) {
      for (Property p : properties.get(0).getProperty()) {
        org.gradoop.common.model.impl.properties.Property prop
          = new org.gradoop.common.model.impl.properties.Property();

        prop.setKey(p.getName());
        value = new PropertyValue();
        String type =
          csv.getColumns().getColumn().get(p.getColumnId()).getType().value();

        switch (type) {
        case "String":
          value.setString(fields[p.getColumnId()]);
          break;
        case "Integer":
          value.setInt(Integer.parseInt(fields[p.getColumnId()]));
          break;
        case "Long":
          value.setLong(Long.parseLong(fields[p.getColumnId()]));
          break;
        case "Float":
          value.setFloat(Float.parseFloat(fields[p.getColumnId()]));
          break;
        case "Double":
          value.setDouble(Double.parseDouble(fields[p.getColumnId()]));
          break;
        case "Boolean":
          value.setBoolean(Boolean.parseBoolean(fields[p.getColumnId()]));
          break;
        default:
          value.setString(fields[p.getColumnId()]);
          break;
        }
        prop.setValue(value);
        list.set(prop);
      }
    }
    return list;
  }
}
