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
import org.gradoop.flink.io.impl.csv.pojos.*;
import org.gradoop.flink.io.impl.csv.tuples.ReferenceTuple;

import java.util.List;
import java.util.regex.Pattern;



public class CSVToElement implements
  FlatMapFunction<Tuple2<Csv, List<String>>, EPGMElement> {

  private EPGMElement reuse;

  private GraphHeadFactory graphHeadFactory;
  private VertexFactory vertexFactory;
  private EdgeFactory edgeFactory;


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

    for (String line : content) {
      String[] fields = line.split(Pattern.quote(csv.getSeparator()));
      if (csv.getGraphhead() != null) {
        reuse = createGraphHead(csv, fields);
      } else if (csv.getVertex() != null) {
        reuse = createVertex(csv, fields);
        if (csv.getVertex().getEdges() != null) {
          for (Vertexedge vertexEdge : csv.getVertex().getEdges().getVertexedge()) {
            collector.collect(createEdge(csv, fields, vertexEdge,
              reuse.getPropertyValue(CSVConstants.PROPERTY_KEY_KEY).getString
                ()));
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




  private org.gradoop.common.model.impl.pojo.Vertex createVertex(Csv csv, String[]
    fields) {
    String label = "";
    reuse = vertexFactory.createVertex();
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

  private org.gradoop.common.model.impl.pojo.Edge createEdge(Csv csv,
    String[] fields) {
    return createEdge(csv, fields, null, "");
  }

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

    if (sourceKey.equals("")) {
      referenceTuple = this.setNamesAndIds(csv.getEdge().getSource(), fields,
        csv.getDatasourceName(), csv.getDomainName(), className);
      sourceKey = createKey(referenceTuple);
    }

    referenceTuple = this.setNamesAndIds(
      edge.getTarget(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);

    String targetKey = createKey(referenceTuple);

    properties.set(CSVConstants.PROPERTY_KEY_SOURCE, sourceKey);
    properties.set(CSVConstants.PROPERTY_KEY_TARGET, targetKey);

    return edgeFactory.createEdge(label, GradoopId.get(), GradoopId.get(), properties);
  }

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
        .replaceAll(CSVConstants.SEPARATOR_GRAPHS, CSVConstants.ESCAPE_REPLACEMENT_GRAPHS));
    }
    return sb.toString();
  }

  private ReferenceTuple setNamesAndIds(Staticorreference staticOrReference,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId((staticOrReference.getStatic() != null) ?
      staticOrReference.getStatic().getName() : "");

    boolean refSet = false;
    for (Ref ref : staticOrReference.getRef()) {
      tuple.setId(tuple.getId() + fields[ref.getColumnId().intValue()]);
      refSet = true;
    }

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

  private ReferenceTuple setNamesAndIds(Objectreferences objectReferences,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId("");

    boolean refSet = false;
    for (Object object : objectReferences.getStaticOrRefOrReference()) {
      if (Static.class.isInstance(object)) {
        tuple.setId(((Static)object).getName());
      } else  if (Ref.class.isInstance(object)) {
        tuple.setId(
          tuple.getId() + fields[((Ref)object).getColumnId().intValue()]);
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
            .replaceAll(CSVConstants.SEPARATOR_LABEL, CSVConstants.ESCAPE_REPLACEMENT_LABEL);
        } else {
          contentString += fields[((Ref) object).getColumnId().intValue()];
        }
      }
    }
    return contentString;
  }


  private String createLabel(Label label, String[] fields) {
    String labelSeparator = (label.getSeparator() == null) ?
      CSVConstants.SEPARATOR_LABEL : label.getSeparator();
    return getEntriesFromStaticOrRef(label.getContent(),
      fields, labelSeparator);
  }

  private String createKey(ReferenceTuple tuple) {

    String newId = tuple.getDatasourceName().replaceAll(CSVConstants.SEPARATOR_KEY,
      CSVConstants.ESCAPE_REPLACEMENT_KEY) + "_" +
      tuple.getDomainName().replaceAll(CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY)
      + "_" +
      tuple.getClassName().replaceAll(CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY)
      + "_" +
      tuple.getId().replaceAll(CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_REPLACEMENT_KEY);

    return newId;
  }


  private PropertyList createProperties(Csv csv, List<Properties> properties,
    Key key, String[] fields) {
    PropertyList list = PropertyList.create();
    String resultKey = createKey(
      new ReferenceTuple(csv.getDatasourceName(), csv.getDomainName(),
        key.getClazz(), fields[0]));

    PropertyValue value = new PropertyValue();
    value.setString(resultKey);

    list.set(CSVConstants.PROPERTY_KEY_KEY, value);

    if (properties != null && !properties.isEmpty()) {
      for (Property p : properties.get(0).getProperty()) {
        org.gradoop.common.model.impl.properties.Property prop = new org.gradoop.common.model.impl.properties.Property();

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
