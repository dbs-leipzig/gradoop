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
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;
import org.gradoop.flink.io.impl.csv.pojos.Graph;
import org.gradoop.flink.io.impl.csv.pojos.Key;
import org.gradoop.flink.io.impl.csv.pojos.Ref;
import org.gradoop.flink.io.impl.csv.pojos.Reference;
import org.gradoop.flink.io.impl.csv.pojos.Static;
import org.gradoop.flink.io.impl.csv.pojos.Staticorreference;
import org.gradoop.flink.io.impl.csv.pojos.Vertexedge;
import org.gradoop.flink.io.impl.csv.pojos.Label;
import org.gradoop.flink.io.impl.csv.pojos.Objectreferences;
import org.gradoop.flink.io.impl.csv.pojos.Property;
import org.gradoop.flink.io.impl.csv.tuples.ReferenceTuple;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Creates EPGMElements from a csv object which contains the meta information
 * to the corresponding content.
 */
public class CSVToElement implements FlatMapFunction<Tuple2<CsvExtension, String>, EPGMElement> {
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
  public CSVToElement(GraphHeadFactory graphHeadFactory, VertexFactory vertexFactory,
    EdgeFactory edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(Tuple2<CsvExtension, String> tuple, Collector<EPGMElement> collector)
    throws Exception {
    CsvExtension csv = tuple.f0;
    String content = tuple.f1;
    //the single values of each line
    String[] fields = content.split(Pattern.quote(csv.getSeparator()));
    //create the graphhead
    if (csv.getGraphhead() != null) {
      collector.collect(createGraphHead(csv, fields));
    }
    //create the vertex
    if (csv.getVertex() != null) {
      EPGMElement vertex = createVertex(csv, fields);
      //if the vertex also defines outgoing edges they are collected too
      if (csv.getVertex().getEdges() != null) {
        for (Vertexedge vertexEdge : csv.getVertex().getEdges().getVertexedge()) {
          //creates and collects an edge with the current vertex as source
          collector.collect(createEdge(csv, fields, vertexEdge, vertex.getPropertyValue(
              CSVConstants.PROPERTY_KEY_KEY).getString()));
        }
      }
      collector.collect(vertex);
    }
    //create the edge
    if (csv.getEdge() != null) {
      collector.collect(createEdge(csv, fields));
    }
  }

  /**
   * Creates a graph head from the given fields by using the csv meta information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return GraphHead
   */
  private org.gradoop.common.model.impl.pojo.GraphHead createGraphHead(CsvExtension csv,
    String[] fields) {
    String label = "";
    List<Property> propertiesCsv = null;
    //if the label is set in the meta file it is read from csv line
    if (csv.getGraphhead().getLabel() != null) {
      label = createLabel(csv.getGraphhead().getLabel(), fields);
    }
    //contains information about the actual class (e.g. tablename) and the actual id (e.g.
    // primary key)
    Key key = csv.getGraphhead().getKey();
    if (csv.getGraphhead().getProperties() != null) {
      //meta information about the properties
      propertiesCsv = csv.getGraphhead().getProperties().getProperty();
    }
    //if there are properties set in the meta file they are read from the csv line
    Properties properties = createProperties(csv, propertiesCsv, key, fields);
    return graphHeadFactory.createGraphHead(label, properties);
  }

  /**
   * Creates a vertex from the given fields by using the csv meta information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return Vertex
   */
  private org.gradoop.common.model.impl.pojo.Vertex createVertex(CsvExtension csv,
    String[] fields) {
    String label = "";
    List<Property> propertiesCsv = null;
    //if the label is set in the meta file it is read from csv line
    if (csv.getVertex().getLabel() != null) {
      label = createLabel(csv.getVertex().getLabel(), fields);
    }
    //contains information about the actual class (e.g. tablename) and the actual id (e.g.
    //primary key)
    Key key = csv.getVertex().getKey();
    if (csv.getVertex().getProperties() != null) {
      //meta information about the properties
      propertiesCsv = csv.getVertex().getProperties().getProperty();
    }
    //class name (e.g. tablename)
    String className = key.getClazz();
    //meta information about the graphs the vertex is contained in
    List<Graph> graphs = csv.getVertex().getGraphs().getGraph();
    //List containing the keys of graphs the vertex is part of
    String graphList = createGraphList(graphs, csv.getDatasourceName(), csv.getDomainName(),
      className, fields);
    //if there are properties set in the meta file they are read from the csv line
    Properties properties = createProperties(csv, propertiesCsv, key, fields);
    properties.set(CSVConstants.PROPERTY_KEY_GRAPHS, graphList);

    return vertexFactory.createVertex(label, properties);
  }

  /**
   * Creates an edge from the given fields by using the csv meta information.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @return Edge
   */
  private org.gradoop.common.model.impl.pojo.Edge createEdge(CsvExtension csv,
    String[] fields) {
    //'null' and "" to create a normal edge and not one initialized by a vertex
    return createEdge(csv, fields, null, "");
  }

  /**
   * Creates an edge from the given fields by using the csv meta information.
   * May be called to create an edge directly from an vertex.
   *
   * @param csv contains meta information
   * @param fields contains the data
   * @param edge vertex edge defined inside the vertex field of the csv
   * @param fullKey the concatenated key of the source vertex
   * @return Edge
   */
  private org.gradoop.common.model.impl.pojo.Edge createEdge(CsvExtension csv,
    String[] fields, Vertexedge edge, String fullKey) {
    String label = "";
    List<Property> propertiesCsv = null;
    //in this case it is a normal edge and none initialized by a vertex
    if (edge == null) {
      edge = csv.getEdge();
    }
    //if the label is set in the meta file it is read from csv line
    if (edge.getLabel() != null) {
      label = createLabel(edge.getLabel(), fields);
    }
    //contains information about the actual class (e.g. tablename) and the actual id (e.g.
    //primary key)
    Key key = edge.getKey();
    if (edge.getProperties() != null) {
      //meta information about the properties
      propertiesCsv = edge.getProperties().getProperty();
    }
    //class name (e.g. tablename)
    String className = edge.getKey().getClazz();
    //meta information about the graphs the vertex is contained in
    List<Graph> graphs = edge.getGraphs().getGraph();
    //List containing the keys of graphs the edge is part of
    String graphList = createGraphList(graphs, csv.getDatasourceName(), csv.getDomainName(),
      className, fields);
    //if there are properties set in the meta file they are read from the csv line
    Properties properties = createProperties(csv, propertiesCsv, key, fields);
    properties.set(CSVConstants.PROPERTY_KEY_GRAPHS, graphList);
    //tuple used to create the source and the target key
    ReferenceTuple referenceTuple;
    // normal edge, so information about the source can be read by the csv-edge meta information
    if (fullKey.equals("")) {
      //relevant key information for the source
      referenceTuple = createKeyTuple(csv.getEdge().getSource(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);
      fullKey = createKey(referenceTuple);
    }
    //write the source key content as edge property
    properties.set(CSVConstants.PROPERTY_KEY_SOURCE, fullKey);
    //relevant key information for the target
    referenceTuple = createKeyTuple(edge.getTarget(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);
    fullKey = createKey(referenceTuple);
    //write the target key content as edge property
    properties.set(CSVConstants.PROPERTY_KEY_TARGET, fullKey);

    return edgeFactory.createEdge(label, GradoopId.get(), GradoopId.get(), properties);
  }

  /**
   * Creates a key which is defined by all needed parameters.
   *
   * @param graphs list of graphs
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @param fields contains the data
   * @return concatenated string representing the key
   */
  private String createGraphList(List<Graph> graphs, String datasourceName, String domainName,
    String className, String[] fields) {
    StringBuilder sb = new StringBuilder();
    boolean notFirst = false;
    for (Graph graph : graphs) {
      //set the separator in front of each graphs key except the first
      if (!notFirst) {
        notFirst = true;
      } else {
        sb.append(CSVConstants.SEPARATOR_GRAPHS);
      }
      //adds a key of a graph to the 'list'
      sb.append(createKey(createGraphTuple(graph, fields, datasourceName, domainName, className))
        .replaceAll(CSVConstants.SEPARATOR_GRAPHS, CSVConstants.ESCAPE_SEPARATOR_GRAPHS));
    }
    return sb.toString();
  }

  /**
   * Creates a tuple which contains the datasource name, the domain name, the class name and the id.
   *
   * @param staticOrReference contains the id information
   * @param fields contains the data
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @return tuple containing all relevant information
   */
  private ReferenceTuple createKeyTuple(Staticorreference staticOrReference, String[] fields,
    String datasourceName, String domainName, String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    //hierarchy information
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
    //extends id by all external reference
    for (Reference reference : staticOrReference.getReference()) {
      if (reference.getDatasourceName() != null) {
        tuple.setDatasourceName(reference.getDatasourceName());
      }
      if (reference.getDomainName() != null) {
        tuple.setDomainName(reference.getDomainName());
      }
      if (refSet) {
        tuple.setClassName(tuple.getClassName() + reference.getKey().getClazz());
      } else {
        tuple.setClassName(reference.getKey().getClazz());
      }
      tuple.setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference.getKey().getContent(),
        fields, CSVConstants.SEPARATOR_ID));
    }
    return tuple;
  }

  /**
   * Creates a Tuple which contains the datasource name, the domain name, the class name and the id.
   *
   * @param objectReferences contains objects with the id information
   * @param fields contains the data
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @return tuple containing all relevant information
   */
  private ReferenceTuple createGraphTuple(Objectreferences objectReferences, String[] fields,
    String datasourceName, String domainName, String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    //hierarchy information
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId("");
    //contains only object, but information extraction depends on their class
    boolean refSet = false;
    for (Object object : objectReferences.getStaticOrRefOrReference()) {
      //static predefined id
      if (Static.class.isInstance(object)) {
        tuple.setId(((Static) object).getName());
        //checks for internal references
      } else  if (Ref.class.isInstance(object)) {
        tuple.setId(tuple.getId() + fields[((Ref) object).getColumnId().intValue()]);
        refSet = true;
        //checks for external references
      } else if (Reference.class.isInstance(object)) {
        Reference reference = (Reference) object;
        if (reference.getDatasourceName() != null) {
          tuple.setDatasourceName(reference.getDatasourceName());
        }
        if (reference.getDomainName() != null) {
          tuple.setDomainName(reference.getDomainName());
        }
        if (refSet) {
          tuple.setClassName(tuple.getClassName() + reference.getKey().getClazz());
        } else {
          tuple.setClassName(reference.getKey().getClazz());
        }
        tuple.setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference.getKey().getContent(),
            fields, CSVConstants.SEPARATOR_ID));
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
  private String getEntriesFromStaticOrRef(List<Serializable> objects, String[] fields,
    String separator) {
    String contentString = "";
    String fieldContent;
    boolean notFirst = false;
    boolean hasSeparator = !separator.equals("");
    for (Object object : objects) {
      if (Static.class.isInstance(object)) {
        contentString = ((Static) object).getName();
      } else if (Ref.class.isInstance(object)) {
        fieldContent = fields[((Ref) object).getColumnId().intValue()];
        //used to either replace all label separators in case a labels content is needed
        switch (separator) {
        case CSVConstants.SEPARATOR_LABEL:
          fieldContent = fieldContent.replaceAll(CSVConstants.SEPARATOR_LABEL,
            CSVConstants.ESCAPE_SEPARATOR_LABEL);
          break;
        //or replace all id separators in case an ids content is needed
        case CSVConstants.SEPARATOR_ID:
          fieldContent = fieldContent.replaceAll(CSVConstants.SEPARATOR_ID,
            CSVConstants.ESCAPE_SEPARATOR_ID);
          break;
        default:
          break;
        }
        //add separator in front of each added content except the first one
        if (notFirst && hasSeparator) {
          contentString += separator;
        } else {
          notFirst = true;
        }
        contentString += fieldContent;
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
    return getEntriesFromStaticOrRef(label.getContent(), fields, labelSeparator);
  }

  /**
   * Creates a string representation of the key which is defined by the
   * datasource name, the domain name, the class name and the id.
   *
   * @param tuple contains all relevant information
   * @return string representation of the key
   */
  private String createKey(ReferenceTuple tuple) {
    StringBuilder sb = new StringBuilder();
    //datasource name
    sb.append(tuple.getDatasourceName().replaceAll(CSVConstants.SEPARATOR_KEY,
      CSVConstants.ESCAPE_SEPARATOR_KEY));
    sb.append(CSVConstants.SEPARATOR_KEY);
    //domain name
    sb.append(tuple.getDomainName().replaceAll(
      CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_SEPARATOR_KEY));
    sb.append(CSVConstants.SEPARATOR_KEY);
    //class name
    sb.append(tuple.getClassName().replaceAll(
      CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_SEPARATOR_KEY));
    sb.append(CSVConstants.SEPARATOR_ID_START);
    //ids
    sb.append(tuple.getId().replaceAll(
      CSVConstants.SEPARATOR_ID_START, CSVConstants.ESCAPE_SEPARATOR_ID_START));
    return sb.toString();
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
  private Properties createProperties(CsvExtension csv, List<Property> properties,
    Key key, String[] fields) {
    Properties list = Properties.create();
    //add a property which contains the key defined by datasource name,...
    String resultKey = createKey(
      new ReferenceTuple(csv.getDatasourceName(), csv.getDomainName(), key.getClazz(),
        getEntriesFromStaticOrRef(key.getContent(), fields, CSVConstants.SEPARATOR_ID)));
    PropertyValue value = new PropertyValue();
    value.setString(resultKey);
    list.set(CSVConstants.PROPERTY_KEY_KEY, value);

    //load all properties and set their type according to the type specified in the meta information
    if (properties != null && !properties.isEmpty()) {
      for (Property p : properties) {
        //gradoop property and not the one defined by the xsd
        org.gradoop.common.model.impl.properties.Property prop
          = new org.gradoop.common.model.impl.properties.Property();

        prop.setKey(p.getName());
        value = new PropertyValue();
        String type = csv.getColumns().getColumn().get(p.getColumnId()).getType().value();
        //set the properties dependent on their type specified in the xsd
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
        // by default the value is stored as string
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
