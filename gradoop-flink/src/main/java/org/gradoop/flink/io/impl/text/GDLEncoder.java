/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.text;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encodes data structures into GDL format.
 */
public class GDLEncoder {
  /**
   * Marks the beginning of the definition of vertices and edges.
   */
  private static final String GRAPH_ELEMENTS_DEFINITION_START = "[";
  /**
   * Marks the end of the definition of vertices and edges.
   */
  private static final String GRAPH_ELEMENTS_DEFINITION_END = "]";
  /**
   * Seperates the id and label of graph, vertex and edge.
   */
  private static final String ID_LABEL_SEPARATOR = ":";
  /**
   * Graph variable prefix, because Gradoop can't parse GDL variable starting with a number.
   */
  private static final String GRAPH_VARIABLE_PREFIX = "g";
  /**
   * vertex variable prefix, because Gradoop can't parse GDL variable starting with a number.
   */
  private static final String VERTEX_VARIABLE_PREFIX = "v";
  /**
   * Marks the end of the properties prefix.
   */
  private static final String PROPERTIES_PREFIX = "{";
  /**
   * Marks the end of the properties string.
   */
  private static final String PROPERTIES_SUFFIX = "}";
  /**
   * Seperates properties.
   */
  private static final String PROPERTIES_SEPARATOR = ",";
  /**
   * Seperates key and value for properties.
   */
  private static final String KEY_VALUE_SEPARATOR = ":";
  /**
   * Suffix for GDL double representation.
   */
  private static final String DOUBLE_SUFFIX = "d";
  /**
   * Suffix for GDL float representation.
   */
  private static final String FLOAT_SUFFIX = "f";
  /**
   * Suffix for GDL long representation.
   */
  private static final String LONG_SUFFIX = "L";
  /**
   * GDL null representation.
   */
  private static final String NULL_STRING = "NULL";
  /**
   * GDL string prefix
   */
  private static final String STRING_PREFIX = "\"";
  /**
   * GDL string suffix
   */
  private static final String STRING_SUFFIX = "\"";

  /**
   * Graph head to encode.
   */
  private GraphHead graphHead;
  /**
   * Vertices to encode.
   */
  private List<Vertex> vertices;
  /**
   * Edges to encode.
   */
  private List<Edge> edges;

  /**
   *
   * @param graphHead graph head that should be encoded
   * @param vertices vertices that should be encoded
   * @param edges edges that should be encoded
   */
  public GDLEncoder(GraphHead graphHead, List<Vertex> vertices, List<Edge> edges) {
    this.graphHead = graphHead;
    this.vertices = vertices;
    this.edges = edges;
  }

  /**
   * Transforms the graph that is represented by graph head, vertices and edges into a GDL string.
   * @return GDL formatted String.
   */
  public String graphToGDLString() {

    Map<GradoopId, String> idToVertexName = getVertexNameMapping(vertices);


    String graphHeadString = graphHeadToGDLString(graphHead);

    String verticesString = vertices.stream()
      .map(v -> vertexToGDLString(v, idToVertexName.get(v.getId())))
      .collect(Collectors.joining("\n"));

    String edgesString = edges.stream()
      .map(e -> edgeToGDLString(e, idToVertexName))
      .collect(Collectors.joining("\n"));

    StringBuilder result = new StringBuilder()
      .append(graphHeadString)
      .append(GRAPH_ELEMENTS_DEFINITION_START).append("\n")
      .append(verticesString).append("\n")
      .append(edgesString).append("\n")
      .append(GRAPH_ELEMENTS_DEFINITION_END);

    return result.toString();
  }

  /**
   * Returns a mapping between the vertex gradoop id and the gdl variable name.
   * @param vertices The graph vertices.
   * @return Mapping between vertex and gdl variable name.
   */
  private Map<GradoopId, String> getVertexNameMapping(List<Vertex> vertices) {
    Map<GradoopId, String> idToVertexName = new HashMap<>();
    for (int i = 0; i < vertices.size(); i++) {
      Vertex v = vertices.get(i);
      String vName = String.format("%s_%s_%s", VERTEX_VARIABLE_PREFIX, v.getLabel(), i);
      idToVertexName.put(v.getId(), vName);
    }
    return idToVertexName;
  }

  /**
   * Returns a GDL formatted graph head string.
   * @param g graph head
   * @return gdl formatted string
   */
  private String graphHeadToGDLString(GraphHead g) {
    StringBuilder result = new StringBuilder()
      .append(GRAPH_VARIABLE_PREFIX)
      .append(graphHead.getId())
      .append(ID_LABEL_SEPARATOR)
      .append(graphHead.getLabel()).append(" ")
      .append(propertiesToGDLString(g.getProperties()));

    return result.toString();
  }
  /**
   * Returns a GDL formatted edge string.
   *
   * @param e The edge to be formatted.
   * @param idToVertexName Maps GradoopId of a vertex to a string that represents the gdl
   *                       variable name
   * @return A GDL formatted edge string.
   */
  private String edgeToGDLString(Edge e, Map<GradoopId, String> idToVertexName) {
    return String.format("(%s)-[:%s%s]->(%s)",
      idToVertexName.get(e.getSourceId()),
      e.getLabel(),
      propertiesToGDLString(e.getProperties()),
      idToVertexName.get(e.getTargetId()));
  }
  /**
   * Returns a GDL formatted vertex.
   *
   * @param v The vertex that should be formatted.
   * @param referenceIdentifier Determines the GDL variable name for the vertex.
   * @return A GDL formatted vertex string.
   */
  private String vertexToGDLString(Vertex v, String referenceIdentifier) {
    return String.format("(%s:%s %s)",
      referenceIdentifier,
      v.getLabel(),
      propertiesToGDLString(v.getProperties()));
  }
  /**
   * Returns the properties as a GDL formatted String.
   *
   * @param properties The properties to be formatted.
   * @return A GDL formatted string that represents the properties.
   */
  private String propertiesToGDLString(Properties properties) {
    if (properties == null || properties.isEmpty()) {
      return "";
    } else {
      return properties.toList().stream()
        .map(this::propertyToGDLString)
        .collect(Collectors.joining(PROPERTIES_SEPARATOR, PROPERTIES_PREFIX, PROPERTIES_SUFFIX));
    }
  }

  /**
   * Returns this property as a GDL formatted String.
   *
   * @param property The property.
   * @return A GDL formatted string that represents the property.
   */
  private String propertyToGDLString(Property property) {
    StringBuilder result = new StringBuilder()
      .append(property.getKey())
      .append(KEY_VALUE_SEPARATOR);

    PropertyValue value = property.getValue();

    if (value.isString()) {
      result.append(STRING_PREFIX).append(value.toString()).append(STRING_SUFFIX);
    } else if (value.isNull()) {
      result.append(NULL_STRING);
    } else if (value.isDouble()) {
      result.append(value.toString()).append(DOUBLE_SUFFIX);
    } else if (value.isFloat()) {
      result.append(value.toString()).append(FLOAT_SUFFIX);
    } else if (value.isLong()) {
      result.append(value.toString()).append(LONG_SUFFIX);
    } else {
      result.append(value.toString());
    }

    return result.toString();
  }
}
