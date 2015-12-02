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

package org.gradoop.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.count.Count;

/**
 * Aggregate function returning the vertex count of a graph.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class VertexCount
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements AggregateFunction<Long, G, V, E> {

  @Override
  public DataSet<Long> execute(LogicalGraph<G, V, E> graph) {
    return Count.count(graph.getVertices());
  }
}
