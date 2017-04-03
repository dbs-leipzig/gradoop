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

package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Extracts an arbitrary value (e.g. label, property key/value, ...) from a vertex and computes its
 * distribution among all vertices.
 *
 * @param <T> value type
 */
public class VertexValueDistribution<T> extends ValueDistribution<Vertex, T> {
  /**
   * Constructor
   *
   * @param valueFunction extracts a value from a vertex
   */
  public VertexValueDistribution(MapFunction<Vertex, T> valueFunction) {
    super(valueFunction);
  }

  @Override
  public DataSet<WithCount<T>> execute(LogicalGraph graph) {
    return compute(graph.getVertices());
  }
}
