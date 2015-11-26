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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.filterfunctions.ElementInGraphs;

/**
 * Creates a new logical graph containing the overlapping vertex and edge
 * sets of two input graphs. Vertex and edge equality is based on their
 * respective identifiers.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class Overlap<
  V extends EPGMVertex,
  E extends EPGMEdge,
  G extends EPGMGraphHead>
  extends AbstractBinaryGraphToGraphOperator<G, V, E> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<G, V, E> executeInternal(
    LogicalGraph<G, V, E> firstGraph, LogicalGraph<G, V, E> secondGraph) {

    DataSet<G> graphHeads = firstGraph.getGraphHead()
      .union(secondGraph.getGraphHead());

    DataSet<V> newVertexSet = firstGraph.getVertices()
      .filter(new ElementInGraphs<G, V>())
      .withBroadcastSet(
        graphHeads, ElementInGraphs.GRAPH_HEADS);

    DataSet<E> newEdgeSet = firstGraph.getEdges()
      .filter(new ElementInGraphs<G, E>())
      .withBroadcastSet(
        graphHeads, ElementInGraphs.GRAPH_HEADS);

    return LogicalGraph.fromDataSets(
      newVertexSet, newEdgeSet, firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Overlap.class.getName();
  }
}
