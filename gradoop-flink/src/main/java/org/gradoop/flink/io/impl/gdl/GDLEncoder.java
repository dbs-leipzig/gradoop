/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.gdl;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Encodes data structures using the GDL format.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class GDLEncoder<G extends GraphHead, V extends Vertex, E extends Edge> {

  /**
   * Marks the beginning of the definition of vertices and edges.
   */
  private static final String GRAPH_ELEMENTS_DEFINITION_START = "[";
  /**
   * Marks the end of the definition of vertices and edges.
   */
  private static final String GRAPH_ELEMENTS_DEFINITION_END = "]";
  /**
   * graph variable prefix
   */
  private static final String GRAPH_VARIABLE_PREFIX = "g";
  /**
   * vertex variable prefix
   */
  private static final String VERTEX_VARIABLE_PREFIX = "v";
  /**
   * edge variable prefix
   */
  private static final String EDGE_VARIABLE_PREFIX = "e";
  /**
   * Marks the end of the properties prefix.
   */
  private static final String PROPERTIES_PREFIX = "{";
  /**
   * Marks the end of the properties string.
   */
  private static final String PROPERTIES_SUFFIX = "}";
  /**
   * Separates properties.
   */
  private static final String PROPERTIES_SEPARATOR = ",";
  /**
   * Separates key and value for properties.
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
  private List<G> graphHeads;
  /**
   * Vertices to encode.
   */
  private List<V> vertices;
  /**
   * Edges to encode.
   */
  private List<E> edges;

  /**
   * Creates a GDLEncoder using the passed parameters.
   *
   * @param graphHeads graph head that should be encoded
   * @param vertices vertices that should be encoded
   * @param edges edges that should be encoded
   */
  public GDLEncoder(List<G> graphHeads, List<V> vertices, List<E> edges) {
    this.graphHeads = graphHeads;
    this.vertices = vertices;
    this.edges = edges;
  }

  /**
   * Creates a GDL formatted string from the graph heads, vertices and edges.
   *
   * @return GDL formatted string
   */
  public String getGDLString() {
    Map<GradoopId, String> idToGraphHeadName = getGraphHeadNameMapping(graphHeads);
    Map<GradoopId, String> idToVertexName = getVertexNameMapping(vertices);
    Map<GradoopId, String> idToEdgeName = getEdgeNameMapping(edges);

    Set<GradoopId> usedVertexIds = new HashSet<>();
    Set<GradoopId> usedEdgeIds = new HashSet<>();

    StringBuilder result = new StringBuilder();

    for (G graphHead : graphHeads) {
      StringBuilder verticesString = new StringBuilder();
      StringBuilder edgesString = new StringBuilder();

      for (V vertex : vertices) {
        boolean containedInGraph = vertex.getGraphIds().contains(graphHead.getId());
        boolean firstOccurrence = !usedVertexIds.contains(vertex.getId());

        if (containedInGraph) {
          String vertexString = vertexToGDLString(vertex, idToVertexName, firstOccurrence);
          usedVertexIds.add(vertex.getId());
          verticesString.append(vertexString).append(System.lineSeparator());
        }
      }

      for (E edge : edges) {
        if (edge.getGraphIds().contains(graphHead.getId())) {
          boolean firstOccurrence = !usedEdgeIds.contains(edge.getId());
          String edgeString = edgeToGDLString(edge, idToVertexName, idToEdgeName, firstOccurrence);
          usedEdgeIds.add(edge.getId());
          edgesString.append(edgeString).append(System.lineSeparator());
        }
      }

      result
        .append(graphHeadToGDLString(graphHead, idToGraphHeadName))
        .append(GRAPH_ELEMENTS_DEFINITION_START).append(System.lineSeparator())
        .append(verticesString)
        .append(edgesString.length() > 0 ? System.lineSeparator() : "")
        .append(edgesString)
        .append(GRAPH_ELEMENTS_DEFINITION_END)
        .append(System.lineSeparator()).append(System.lineSeparator());
    }
    return result.toString();
  }

  /**
   * Returns a mapping between the graph heads gradoop ids and the GDL variable names.
   *
   * @param graphHeads The graph heads.
   * @return Mapping between graph head and GDL variable name.
   */
  private Map<GradoopId, String> getGraphHeadNameMapping(List<G> graphHeads) {
    Map<GradoopId, String> idToGraphHeadName = new HashMap<>(graphHeads.size());
    for (int i = 0; i < graphHeads.size(); i++) {
      G graphHead = graphHeads.get(i);
      String gName = String.format("%s%s", GRAPH_VARIABLE_PREFIX, i);
      idToGraphHeadName.put(graphHead.getId(), gName);
    }
    return idToGraphHeadName;
  }

  /**
   * Returns a mapping between the vertex GradoopID and the GDL variable name.
   *
   * @param vertices The graph vertices.
   * @return Mapping between vertex and GDL variable name.
   */
  private Map<GradoopId, String> getVertexNameMapping(List<V> vertices) {
    Map<GradoopId, String> idToVertexName = new HashMap<>(vertices.size());
    for (int i = 0; i < vertices.size(); i++) {
      V vertex = vertices.get(i);
      String vName = String.format("%s_%s_%s", VERTEX_VARIABLE_PREFIX, vertex.getLabel(), i);
      idToVertexName.put(vertex.getId(), vName);
    }
    return idToVertexName;
  }

  /**
   * Returns a mapping between the edge GradoopId and the GDL variable name.
   *
   * @param edges The graph edges.
   * @return Mapping between edge and GDL variable name.
   */
  private Map<GradoopId, String> getEdgeNameMapping(List<E> edges) {
    Map<GradoopId, String> idToEdgeName = new HashMap<>(edges.size());
    for (int i = 0; i < edges.size(); i++) {
      E edge = edges.get(i);
      String eName = String.format("%s_%s_%s", EDGE_VARIABLE_PREFIX, edge.getLabel(), i);
      idToEdgeName.put(edge.getId(), eName);
    }
    return idToEdgeName;
  }

  /**
   * Returns a GDL formatted graph head string.
   *
   * @param graphhead graph head
   * @param idToGraphHeadName mapping from graph head id to its GDL variable name
   * @return GDL formatted string
   */
  private String graphHeadToGDLString(G graphhead, Map<GradoopId, String> idToGraphHeadName) {
    return String.format("%s:%s %s",
      idToGraphHeadName.get(graphhead.getId()),
      graphhead.getLabel(),
      propertiesToGDLString(graphhead.getProperties()));
  }

  /**
   * Returns the gdl formatted vertex including the properties and the label on first occurrence
   * or otherwise just the variable name.
   *
   * @param vertex The vertex that should be formatted.
   * @param idToVertexName Maps GradoopId of a vertex to a string that represents the gdl
   *                       variable name
   * @param firstOccurrence Is it the first occurrence of the vertex in all graphs?
   * @return A GDL formatted vertex string.
   */
  private String vertexToGDLString(V vertex, Map<GradoopId, String> idToVertexName,
    boolean firstOccurrence) {
    if (firstOccurrence) {
      return String.format("(%s:%s %s)",
        idToVertexName.get(vertex.getId()),
        vertex.getLabel(),
        propertiesToGDLString(vertex.getProperties()));
    } else {
      return String.format("(%s)", idToVertexName.get(vertex.getId()));
    }
  }

  /**
   * Returns the GDL formatted edge, including the properties and the label on first occurrence
   * or otherwise just the variable name.
   *
   * @param edge The edge to be formatted.
   * @param idToVertexName Maps GradoopId of a vertex to a string that represents the GDL
   *                       variable name
   * @param idToEdgeName Maps GradoopId of an edge to a string that represents the GDL variable
   *                     name.
   * @param firstOccurrence Is it the first occurrence of the edge in all graphs?
   * @return A GDL formatted edge string.
   */
  private String edgeToGDLString(E edge, Map<GradoopId, String> idToVertexName,
    Map<GradoopId, String> idToEdgeName, boolean firstOccurrence) {
    String result;
    if (firstOccurrence) {
      result =  String.format("(%s)-[%s:%s%s]->(%s)",
        idToVertexName.get(edge.getSourceId()),
        idToEdgeName.get(edge.getId()),
        edge.getLabel(),
        propertiesToGDLString(edge.getProperties()),
        idToVertexName.get(edge.getTargetId()));
    } else {
      result = String.format("(%s)-[%s]->(%s)",
        idToVertexName.get(edge.getSourceId()),
        idToEdgeName.get(edge.getId()),
        idToVertexName.get(edge.getTargetId()));
    }
    return result;
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
