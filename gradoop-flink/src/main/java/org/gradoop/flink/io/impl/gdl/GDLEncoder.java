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
package org.gradoop.flink.io.impl.gdl;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
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
  private List<GraphHead> graphHeads;
  /**
   * Vertices to encode.
   */
  private List<Vertex> vertices;
  /**
   * Edges to encode.
   */
  private List<Edge> edges;

  /**
   * Creates a GDLEncoder using the passed parameters.
   *
   * @param graphHeads graph head that should be encoded
   * @param vertices vertices that should be encoded
   * @param edges edges that should be encoded
   */
  public GDLEncoder(List<GraphHead> graphHeads, List<Vertex> vertices, List<Edge> edges) {
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

    for (GraphHead gh : graphHeads) {
      StringBuilder verticesString = new StringBuilder();
      StringBuilder edgesString = new StringBuilder();

      for (Vertex v : vertices) {
        boolean containedInGraph = v.getGraphIds().contains(gh.getId());
        boolean firstOccurrence = !usedVertexIds.contains(v.getId());

        if (containedInGraph && firstOccurrence) {
          String vertexString = vertexToGDLString(v, idToVertexName);
          usedVertexIds.add(v.getId());
          verticesString.append(vertexString).append(System.lineSeparator());
        }
      }

      for (Edge e : edges) {
        if (e.getGraphIds().contains(gh.getId())) {
          boolean firstOccurrence = !usedEdgeIds.contains(e.getId());
          String edgeString = edgeToGDLString(e, idToVertexName, idToEdgeName, firstOccurrence);
          usedEdgeIds.add(e.getId());
          edgesString.append(edgeString).append(System.lineSeparator());
        }
      }

      result
        .append(graphHeadToGDLString(gh, idToGraphHeadName))
        .append(GRAPH_ELEMENTS_DEFINITION_START).append(System.lineSeparator())
        .append(verticesString).append(System.lineSeparator())
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
  private Map<GradoopId, String> getGraphHeadNameMapping(List<GraphHead> graphHeads) {
    Map<GradoopId, String> idToGraphHeadName = new HashMap<>(graphHeads.size());
    for (int i = 0; i < graphHeads.size(); i++) {
      GraphHead g = graphHeads.get(i);
      String gName = String.format("%s%s", GRAPH_VARIABLE_PREFIX, i);
      idToGraphHeadName.put(g.getId(), gName);
    }
    return idToGraphHeadName;
  }

  /**
   * Returns a mapping between the vertex GradoopID and the GDL variable name.
   *
   * @param vertices The graph vertices.
   * @return Mapping between vertex and GDL variable name.
   */
  private Map<GradoopId, String> getVertexNameMapping(List<Vertex> vertices) {
    Map<GradoopId, String> idToVertexName = new HashMap<>(vertices.size());
    for (int i = 0; i < vertices.size(); i++) {
      Vertex v = vertices.get(i);
      String vName = String.format("%s_%s_%s", VERTEX_VARIABLE_PREFIX, v.getLabel(), i);
      idToVertexName.put(v.getId(), vName);
    }
    return idToVertexName;
  }

  /**
   * Returns a mapping between the edge GradoopId and the GDL variable name.
   *
   * @param edges The graph edges.
   * @return Mapping between edge and GDL variable name.
   */
  private Map<GradoopId, String> getEdgeNameMapping(List<Edge> edges) {
    Map<GradoopId, String> idToEdgeName = new HashMap<>(edges.size());
    for (int i = 0; i < edges.size(); i++) {
      Edge e = edges.get(i);
      String eName = String.format("%s_%s_%s", EDGE_VARIABLE_PREFIX, e.getLabel(), i);
      idToEdgeName.put(e.getId(), eName);
    }
    return idToEdgeName;
  }

  /**
   * Returns a GDL formatted graph head string.
   *
   * @param g graph head
   * @param idToGraphHeadName mapping from graph head id to its GDL variable name
   * @return GDL formatted string
   */
  private String graphHeadToGDLString(GraphHead g, Map<GradoopId, String> idToGraphHeadName) {
    return String.format("%s:%s %s",
      idToGraphHeadName.get(g.getId()),
      g.getLabel(),
      propertiesToGDLString(g.getProperties()));
  }

  /**
   * Returns the gdl formatted vertex including the properties and the label on first occurrence
   * or otherwise just the variable name.
   *
   * @param v The vertex that should be formatted.
   * @param idToVertexName Maps GradoopId of a vertex to a string that represents the gdl
   *                       variable name
   * @return A GDL formatted vertex string.
   */
  private String vertexToGDLString(Vertex v, Map<GradoopId, String> idToVertexName)  {
    return String.format("(%s:%s %s)",
      idToVertexName.get(v.getId()),
      v.getLabel(),
      propertiesToGDLString(v.getProperties()));
  }

  /**
   * Returns the GDL formatted edge, including the properties and the label on first occurrence
   * or otherwise just the variable name.
   *
   * @param e The edge to be formatted.
   * @param idToVertexName Maps GradoopId of a vertex to a string that represents the GDL
   *                       variable name
   * @param idToEdgeName Maps GradoopId of an edge to a string that represents the GDL variable
   *                     name.
   * @param firstOccurrence Is it the first occurrence of the vertex in all graphs?
   * @return A GDL formatted edge string.
   */
  private String edgeToGDLString(
    Edge e,
    Map<GradoopId, String> idToVertexName,
    Map<GradoopId, String> idToEdgeName,
    boolean firstOccurrence) {
    String result;
    if (firstOccurrence) {
      result =  String.format("(%s)-[%s:%s%s]->(%s)",
        idToVertexName.get(e.getSourceId()),
        idToEdgeName.get(e.getId()),
        e.getLabel(),
        propertiesToGDLString(e.getProperties()),
        idToVertexName.get(e.getTargetId()));
    } else {
      result = String.format("(%s)-[%s]->(%s)",
        idToVertexName.get(e.getSourceId()),
        idToEdgeName.get(e.getId()),
        idToVertexName.get(e.getTargetId()));
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
