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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.CollectionAggregateFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.graphcontainment.ExpandGraphsToIds;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;

/**
 * Aggregate function returning the vertex count of a graph / graph collection.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class VertexCount
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements
  AggregateFunction<G, V, E, Long>, CollectionAggregateFunction<G, V, E, Long> {

  /**
   * Returns a 1-element dataset containing the vertex count of the given graph.
   *
   * @param graph input graph
   * @return 1-element dataset with vertex count
   */
  @Override
  public DataSet<Long> execute(LogicalGraph<G, V, E> graph) {
    return Count.count(graph.getVertices());
  }

  /**
   * Returns a dataset containing graph identifiers and the corresponding vertex
   * count.
   *
   * @param collection input graph collection
   * @return dataset with graph + vertex count tuples
   */
  @Override
  public DataSet<Tuple2<GradoopId, Long>> execute(
    GraphCollection<G, V, E> collection) {
    return Count.groupBy(
      collection.getVertices().flatMap(new ExpandGraphsToIds<V>()));
  }
}
