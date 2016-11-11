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

package org.gradoop.flink.model.impl.pojos;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.io.Serializable;
import java.util.Map;

/**
 * Traversal optimized representation of a graph transaction.
 * @param <T> type of algorithm specific cell value
 */
public class AdjacencyList<T> implements Serializable {

  /**
   * Graph Id
   */
  private GradoopId graphId;
  
  /**
   * graph / vertex / edge id => label
   */
  private Map<GradoopId, String> labels;

  /**
   * graph / vertex / edge id => properties
   */
  private Map<GradoopId, PropertyList> properties;

  /**
   * vertex id => adjacency list row
   */
  private Map<GradoopId, AdjacencyListRow<T>> rows;

  /**
   * Constructor.
   *
   * @param graphId graph id
   * @param labels graph, vertex and edge labels
   * @param properties graph, vertex and edge properties
   * @param rows adjacency list rows
   */
  public AdjacencyList(GradoopId graphId, Map<GradoopId, String> labels,
    Map<GradoopId, PropertyList> properties, Map<GradoopId, AdjacencyListRow<T>> rows) {
    this.graphId = graphId;
    this.labels = labels;
    this.properties = properties;
    this.rows = rows;
  }

  /**
   * Label accessor.
   *
   * @param elementId graph / vertex / edge id
   */
  public String getLabel(GradoopId elementId) {
    return labels.get(elementId);
  }

  /**
   * Property accessor.
   *
   * @param elementId graph / vertex / edge id
   */
  public PropertyList getProperties(GradoopId elementId) {
    return properties.get(elementId);
  }
  
  /**
   * Convenience method to get the adjacency list of a vertex.
   *
   * @param vertexId vertex id
   * @return adjacency list
   */
  public AdjacencyListRow<T> getRows(GradoopId vertexId) {
    return rows.get(vertexId);
  }

  public GradoopId getGraphId() {
    return graphId;
  }

  public void setGraphId(GradoopId graphId) {
    this.graphId = graphId;
  }
}
