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

package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import java.util.Map;

/**
 * Represents several statistics related to a {@link org.gradoop.flink.model.impl.LogicalGraph}.
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
  private Map<String, Map<String, Long>> distinctEdgePropertyValuesByLabelAndPropertyName;
  /**
   * Number of distinct vertex property values of a given label - property name pair
   */
  private Map<String, Map<String, Long>> distinctVertexPropertyValuesByLabelAndPropertyName;
  /**
   * Number of distinct edge property values for property names
   */
  private Map<String, Long> distinctEdgePropertyValuesByPropertyName;
  /**
   * Number of distinct vertex property values for property names
   */
  private Map<String, Long> distinctVertexPropertyValuesByPropertyName;

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
   * @param distinctEdgePropertyValuesByLabelAndPropertyName (label,property) -> distinct values
   * @param distinctVertexPropertyValuesByLabelAndPropertyName (label,property) -> distinct values
   * @param distinctEdgePropertyValuesByPropertyName (edge property) -> distinct values
   * @param distinctVertexPropertyValuesByPropertyName (vertex property) -> distinct values
   */
  GraphStatistics(long vertexCount, long edgeCount, Map<String, Long> vertexCountByLabel,
    Map<String, Long> edgeCountByLabel,
    Map<String, Map<String, Long>> edgeCountBySourceVertexAndEdgeLabel,
    Map<String, Map<String, Long>> edgeCountByTargetVertexAndEdgeLabel,
    long distinctSourceVertexCount, long distinctTargetVertexCount,
    Map<String, Long> distinctSourceVertexCountByEdgeLabel,
    Map<String, Long> distinctTargetVertexCountByEdgeLabel,
    Map<String, Map<String, Long>> distinctEdgePropertyValuesByLabelAndPropertyName,
    Map<String, Map<String, Long>> distinctVertexPropertyValuesByLabelAndPropertyName,
    Map<String, Long> distinctEdgePropertyValuesByPropertyName,
    Map<String, Long> distinctVertexPropertyValuesByPropertyName) {
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
    this.distinctEdgePropertyValuesByLabelAndPropertyName =
      distinctEdgePropertyValuesByLabelAndPropertyName;
    this.distinctVertexPropertyValuesByLabelAndPropertyName =
      distinctVertexPropertyValuesByLabelAndPropertyName;
    this.distinctEdgePropertyValuesByPropertyName = distinctEdgePropertyValuesByPropertyName;
    this.distinctVertexPropertyValuesByPropertyName = distinctVertexPropertyValuesByPropertyName;
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
  public long getVertexCountByLabel(String vertexLabel) {
    return vertexCountByLabel.getOrDefault(vertexLabel, 0L);
  }

  /**
   * Returns the number of edges with the specified label.
   *
   * @param edgeLabel edge label
   * @return number of edges with the given label
   */
  public long getEdgeCountByLabel(String edgeLabel) {
    return edgeCountByLabel.getOrDefault(edgeLabel, 0L);
  }

  /**
   * Returns the number of edges with a specified source vertex label and edge label.
   *
   * @param vertexLabel source vertex label
   * @param edgeLabel edge label
   * @return number of edges with the specified labels
   */
  public long getEdgeCountBySourceVertexAndEdgeLabel(String vertexLabel, String edgeLabel) {
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
  public long getEdgeCountByTargetVertexAndEdgeLabel(String vertexLabel, String edgeLabel) {
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
   * @param label edge label
   * @return number of distinct source vertices incident to the labeled edge
   */
  public long getDistinctSourceVertexCountByEdgeLabel(String label) {
    return distinctSourceVertexCountByEdgeLabel.getOrDefault(label, 0L);
  }

  /**
   * Returns the number of distinct target vertices incident to an edge with the specified label.
   *
   * @param label edge label
   * @return number of distinct target vertices incident to the labeled edge
   */
  public long getDistinctTargetVertexCountByEdgeLabel(String label) {
    return distinctTargetVertexCountByEdgeLabel.getOrDefault(label, 0L);
  }

  /**
   * Returns the number of distinct property values for given edge label property name pair
   * Eg (Person, name) -> 20
   *
   * @param label label
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctEdgePropertyValuesByLabelAndPropertyName(String label,
    String propertyName) {
    return distinctEdgePropertyValuesByLabelAndPropertyName.containsKey(label) ?
      distinctEdgePropertyValuesByLabelAndPropertyName
        .get(label).getOrDefault(propertyName, 0L) : 0;
  }

  /**
   * Returns the number of distinct property values for given vertex label property name pair
   * Eg (Person, name) -> 20
   *
   * @param label label
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctVertexPropertyValuesByLabelAndPropertyName(String label,
    String propertyName) {
    return distinctVertexPropertyValuesByLabelAndPropertyName.containsKey(label) ?
      distinctVertexPropertyValuesByLabelAndPropertyName
        .get(label).getOrDefault(propertyName, 0L) : 0;
  }

  /**
   * Returns the number of distinct edge property values for given property name
   * Eg (name) -> 20
   *
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctEdgePropertyValuesByPropertyName(String propertyName) {
    return distinctEdgePropertyValuesByPropertyName.getOrDefault(propertyName, 0L);
  }

  /**
   * Returns the number of distinct vertex property values for given property name
   * Eg (name) -> 20
   *
   * @param propertyName property name
   * @return number of distinct property values for label property name pair
   */
  public long getDistinctVertexPropertyValuesByPropertyName(String propertyName) {
    return distinctVertexPropertyValuesByPropertyName.getOrDefault(propertyName, 0L);
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("GraphStatistics{");
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
    sb.append(String.format(",%n distinctEdgePropertyValuesByLabelAndPropertyName="))
      .append(distinctEdgePropertyValuesByLabelAndPropertyName);
    sb.append(String.format(",%n distinctVertexPropertyValuesByLabelAndPropertyName="))
      .append(distinctVertexPropertyValuesByLabelAndPropertyName);
    sb.append(String.format(",%n distinctEdgePropertyValuesByPropertyName="))
      .append(distinctEdgePropertyValuesByPropertyName);
    sb.append(String.format(",%n distinctVertexPropertyValuesByPropertyName="))
      .append(distinctVertexPropertyValuesByPropertyName);
    sb.append(String.format("%n}"));
    return sb.toString();
  }
}
