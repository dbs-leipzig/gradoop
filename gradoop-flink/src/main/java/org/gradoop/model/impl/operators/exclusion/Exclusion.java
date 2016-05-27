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

/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or transform
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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.model.impl.functions.utils.LeftSide;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Computes the exclusion graph from two logical graphs.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Exclusion
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryGraphToGraphOperator<G, V, E> {

  /**
   * Creates a new logical graph containing only vertices and edges that exist
   * in the first input graph but not in the second input graph. Vertex and edge
   * equality is based on their respective identifiers.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return first graph without elements from second graph
   */
  @Override
  public LogicalGraph<G, V, E> execute(
    LogicalGraph<G, V, E> firstGraph, LogicalGraph<G, V, E> secondGraph) {

    DataSet<GradoopId> graphId = secondGraph.getGraphHead().map(new Id<G>());

    DataSet<V> newVertexSet = firstGraph.getVertices()
      .filter(new NotInGraphBroadcast<V>())
      .withBroadcastSet(graphId, NotInGraphBroadcast.GRAPH_ID);

    DataSet<E> newEdgeSet = firstGraph.getEdges()
      .filter(new NotInGraphBroadcast<E>())
      .withBroadcastSet(graphId, NotInGraphBroadcast.GRAPH_ID)
      .join(newVertexSet)
      .where(new SourceId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>())
      .join(newVertexSet)
      .where(new TargetId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>());

    return LogicalGraph.fromDataSets(
      newVertexSet, newEdgeSet, firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Exclusion.class.getName();
  }
}
