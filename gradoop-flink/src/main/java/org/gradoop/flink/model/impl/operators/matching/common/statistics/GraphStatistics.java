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
package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.Collections;
import java.util.Map;

/**
 * Represents several statistics related to a {@link LogicalGraph}.
 */
public class GraphStatistics {
  /**
   * Number of vertices
   */
  private long vertexCount;
  /**
   * Number of edges
   */
  private long edgeCount;
  /**
   * Frequency distribution for vertex labels
   */
  private Map<String, Long> vertexCountByLabel;
  /**
   * Frequency distribution for edge labels
   */
  private Map<String, Long> edgeCountByLabel;
  /**
   * Number of edges with a specific source vertex label and edge label.
   */
  private Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel;
  /**
   * Number of edges with a specific target vertex label and edge label.
   */
  private Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel;
  /**
   * Number of distinct source vertices, i.e. the number of distinct vertices that have at least
   * one outgoing edge.
   */
  private long distinctSourceVertexCount;
  /**
   * Number of distinct target vertices, i.e. the number of distinct vertices that have at least
   * one incoming edge.
   */
  private long distinctTargetVertexCount;
  /**
   * Number of distinct source vertices by edge label, i.e. the number of distinct vertices that
   * have at least one outgoing edge with the specified label.
   */
  private Map<String, Long> distinctSourceVertexCountByEdgeLabel;
  /**
   * Number of distinct target vertices by edge label, i.e. the number of distinct vertices that
   * have at least one incoming edge with the specified label.
   */
  private Map<String, Long> distinctTargetVertexCountByEdgeLabel;
  /**
   * Number of distinct edge property values of a given label - property name pair
   */
  private Map<String, Map<String, Long>> distinctEdgePropertiesByLabel;
  /**
   * Number of distinct vertex property values of a given label - property name pair
   */
  private Map<String, Map<String, Long>> distinctVertexPropertiesByLabel;
  /**
   * Number of distinct edge property values for property names
   */
  private Map<String, Long> distinctEdgeProperties;
  /**
   * Number of distinct vertex property values for property names
   */
  private Map<String, Long> distinctVertexProperties;

  /**
   * Constructor using basic statistics.
   *
   * @param vertexCount total number of vertices
   * @param edgeCount total number of edges
   * @param distinctSourceVertexCount number of distinct source vertices
   * @param distinctTargetVertexCount number of distinct target vertices
   */
  public GraphStatistics(long vertexCount, long edgeCount,
    long distinctSourceVertexCount, long distinctTargetVertexCount) {
    this(vertexCount,
      edgeCount,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      distinctSourceVertexCount,
      distinctTargetVertexCount,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap());
  }

  /**
   * Constructor.
   *
   * @param vertexCount number of vertices
   * @param edgeCount number of edges
   * @param vertexCountByLabel number of vertices by label
   * @param edgeCountByLabel number of edges by label
   * @param edgeCountBySourceVertexAndEdgeLabel number of edges by source vertex and edge label
   * @param edgeCountByTargetVertexAndEdgeLabel number of edges by target vertex and edge label
   * @param distinctSourceVertexCount number of distinct source vertices
   * @param distinctTargetVertexCount number of distinct target vertices
   * @param distinctSourceVertexCountByEdgeLabel number of distinct source vertices by edge label
   * @param distinctTargetVertexCountByEdgeLabel number of distinct target vertices by edge label
   * @param distinctEdgePropertiesByLabel (label,property) -> distinct values
   * @param distinctVertexPropertiesByLabel (label,property) -> distinct values
   * @param distinctEdgeProperties (edge property) -> distinct values
   * @param distinctVertexProperties (vertex property) -> distinct values
   */
  GraphStatistics(long vertexCount, long edgeCount, Map<String, Long> vertexCountByLabel,
    Map<String, Long> edgeCountByLabel,
    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel,
    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel,
    long distinctSourceVertexCount, long distinctTargetVertexCount,
    Map<String, Long> distinctSourceVertexCountByEdgeLabel,
    Map<String, Long> distinctTargetVertexCountByEdgeLabel,
    Map<String, Map<String, Long>> distinctEdgePropertiesByLabel,
    Map<String, Map<String, Long>> distinctVertexPropertiesByLabel,
    Map<String, Long> distinctEdgeProperties,
    Map<String, Long> distinctVertexProperties) {
    this.vertexCount = vertexCount;
    this.edgeCount = edgeCount;
    this.vertexCountByLabel = vertexCountByLabel;
    this.edgeCountByLabel = edgeCountByLabel;
    this.edgeCountBySourceVertexAndEdgeLabel = edgeCountBySourceVertexAndEdgeLabel;
    this.edgeCountByTargetVertexAndEdgeLabel = edgeCountByTargetVertexAndEdgeLabel;
    this.distinctSourceVertexCount = distinctSourceVertexCount;
    this.distinctTargetVertexCount = distinctTargetVertexCount;
    this.distinctSourceVertexCountByEdgeLabel = distinctSourceVertexCountByEdgeLabel;
    this.distinctTargetVertexCountByEdgeLabel = distinctTargetVertexCountByEdgeLabel;
    this.distinctEdgePropertiesByLabel = distinctEdgePropertiesByLabel;
    this.distinctVertexPropertiesByLabel = distinctVertexPropertiesByLabel;
    this.distinctEdgeProperties = distinctEdgeProperties;
    this.distinctVertexProperties = distinctVertexProperties;
  }

  /**
   * Returns the number of vertices in the graph.
   *
   * @return vertex count
   */
  public long getVertexCount() {
    return vertexCount;
  }

  /**
   * Returns the number of edges in the graph
   *
   * @return edge count
   */
  public long getEdgeCount() {
    return edgeCount;
  }

  /**
   * Returns the number of vertices with the specified label.
   *
   * @param vertexLabel vertex label
   * @return number of vertices with the given label
   */
  public long getVertexCount(String vertexLabel) {
    return vertexCountByLabel.getOrDefault(vertexLabel, 0L);
  }

  /**
   * Returns the number of edges with the specified label.
   *
   * @param edgeLabel edge label
   * @return number of edges with the given label
   */
  public long getEdgeCount(String edgeLabel) {
    return edgeCountByLabel.getOrDefault(edgeLabel, 0L);
  }

  /**
   * Returns the number of edges with a specified source vertex label and edge label.
   *
   * @param vertexLabel source vertex label
   * @param edgeLabel edge label
   * @return number of edges with the specified labels
   */
  public long getEdgeCountBySource(String vertexLabel, String edgeLabel) {
    return (edgeCountBySourceVertexAndEdgeLabel.containsKey(vertexLabel)) ?
      edgeCountBySourceVertexAndEdgeLabel.get(vertexLabel).getOrDefault(edgeLabel, 0L) :
      0L;
  }

  /**
   * Returns the number of edges with a specified target vertex label and edge label.
   *
   * @param vertexLabel target vertex label
   * @param edgeLabel edge label
   * @return number of edges with the specified labels
   */
  public long getEdgeCountByTarget(String vertexLabel, String edgeLabel) {
    return (edgeCountByTargetVertexAndEdgeLabel.containsKey(vertexLabel)) ?
      edgeCountByTargetVertexAndEdgeLabel.get(vertexLabel).getOrDefault(edgeLabel, 0L) :
      0L;
  }

  /**
   * Returns the number of distinct source vertices.
   *
   * @return number of distinct source vertices
   */
  public long getDistinctSourceVertexCount() {
    return distinctSourceVertexCount;
  }

  /**
   * Returns the number of distinct target vertices.
   *
   * @return number of distinct target vertices
   */
  public long getDistinctTargetVertexCount() {
    return distinctTargetVertexCount;
  }

  /**
   * Returns the number of distinct source vertices incident to an edge with the specified label.
   *
   * @param edgeLabel edge label
   * @return number of distinct source vertices incident to the labeled edge
   */
  public long getDistinctSourceVertexCount(String edgeLabel) {
    return distinctSourceVertexCountByEdgeLabel.getOrDefault(edgeLabel, 0L);
  }

  /**
   * Returns the number of distinct target vertices incident to an edge with the specified label.
   *
   * @param edgeLabel edge label
   * @return number of distinct target vertices incident to the labeled edge
   */
  public long getDistinctTargetVertexCount(String edgeLabel) {
    return distinctTargetVertexCountByEdgeLabel.getOrDefault(edgeLabel, 0L);
  }

  /**
   * Returns the number of distinct vertex property values for given property name
   * Eg (name) -> 20
   *
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctVertexProperties(String propertyName) {
    return distinctVertexProperties.getOrDefault(propertyName, 0L);
  }

  /**
   * Returns the number of distinct edge property values for given property name
   * Eg (name) -> 20
   *
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctEdgeProperties(String propertyName) {
    return distinctEdgeProperties.getOrDefault(propertyName, 0L);
  }

  /**
   * Returns the number of distinct property values for given vertex label property name pair
   * Eg (Person, name) -> 20
   *
   * @param vertexLabel vertex label
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctVertexProperties(String vertexLabel, String propertyName) {
    return distinctVertexPropertiesByLabel.containsKey(vertexLabel) ?
      distinctVertexPropertiesByLabel.get(vertexLabel).getOrDefault(propertyName, 0L) : 0;
  }

  /**
   * Returns the number of distinct property values for given edge label property name pair
   * Eg (Person, name) -> 20
   *
   * @param edgeLabel edge label
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctEdgeProperties(String edgeLabel, String propertyName) {
    return distinctEdgePropertiesByLabel.containsKey(edgeLabel) ?
      distinctEdgePropertiesByLabel.get(edgeLabel).getOrDefault(propertyName, 0L) : 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GraphStatistics{");
    sb.append(String.format("%n vertexCount="))
      .append(vertexCount);
    sb.append(String.format(",%n edgeCount="))
      .append(edgeCount);
    sb.append(String.format(",%n vertexCountByLabel="))
      .append(vertexCountByLabel);
    sb.append(String.format(",%n edgeCountByLabel="))
      .append(edgeCountByLabel);
    sb.append(String.format(",%n edgeCountByTargetVertexAndEdgeLabel="))
      .append(edgeCountByTargetVertexAndEdgeLabel);
    sb.append(String.format(",%n edgeCountBySourceVertexAndEdgeLabel="))
      .append(edgeCountBySourceVertexAndEdgeLabel);
    sb.append(String.format(",%n distinctSourceVertexCount="))
      .append(distinctSourceVertexCount);
    sb.append(String.format(",%n distinctTargetVertexCount="))
      .append(distinctTargetVertexCount);
    sb.append(String.format(",%n distinctSourceVertexCountByEdgeLabel="))
      .append(distinctSourceVertexCountByEdgeLabel);
    sb.append(String.format(",%n distinctTargetVertexCountByEdgeLabel="))
      .append(distinctTargetVertexCountByEdgeLabel);
    sb.append(String.format(",%n distinctVertexProperties="))
      .append(distinctVertexProperties);
    sb.append(String.format(",%n distinctEdgeProperties="))
      .append(distinctEdgeProperties);
    sb.append(String.format(",%n distinctVertexPropertiesByLabel="))
      .append(distinctVertexPropertiesByLabel);
    sb.append(String.format(",%n distinctEdgePropertiesByLabel="))
      .append(distinctEdgePropertiesByLabel);

    sb.append(String.format("%n}"));
    return sb.toString();
  }
}
