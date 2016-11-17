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

package org.gradoop.flink.representation.adjacencylist;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.Map;

/**
 * Traversal optimized representation of a graph transaction.
 * @param <T> type of algorithm specific cell value
 */
public class AdjacencyList<T> extends Tuple4<
  GradoopId,
  Map<GradoopId, String>,
  Map<GradoopId, Properties>,
  Map<GradoopId, AdjacencyListRow<T>>
  > {

  /**
   * Default constructor.
   */
  public AdjacencyList() {
  }

  /**
   * Constructor.
   *
   * @param graphId graph id
   * @param labels graph, vertex and edge labels
   * @param properties graph, vertex and edge properties
   * @param rows adjacency list rows
   */
  public AdjacencyList(GradoopId graphId, Map<GradoopId, String> labels,
    Map<GradoopId, Properties> properties, Map<GradoopId, AdjacencyListRow<T>> rows) {
    this.f0 = graphId;
    this.f1 = labels;
    this.f2 = properties;
    this.f3 = rows;
  }

  /**
   * Label accessor.
   *
   * @param elementId graph / vertex / edge id
   *
   * @return label
   */
  public String getLabel(GradoopId elementId) {
    return f1.get(elementId);
  }

  /**
   * Property accessor.
   * @param elementId graph / vertex / edge id
   *
   * @return property list
   */
  public Properties getProperties(GradoopId elementId) {
    return f2.get(elementId);
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    this.f0 = graphId;
  }

  public Map<GradoopId, AdjacencyListRow<T>> getRows() {
    return f3;
  }
}
