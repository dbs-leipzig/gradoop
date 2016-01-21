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

package org.gradoop.model.impl.operators.modification;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.ModificationFunction;
import org.gradoop.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Applies the modification operator on on all logical graphs in a graph
 * collection.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class ApplyModification
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends Modification<G, V, E>
  implements ApplicableUnaryGraphToGraphOperator<G, V, E> {

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadModFunc graph head modification function
   * @param vertexModFunc    vertex modification function
   * @param edgeModFunc      edge modification function
   */
  public ApplyModification(ModificationFunction<G> graphHeadModFunc,
    ModificationFunction<V> vertexModFunc,
    ModificationFunction<E> edgeModFunc) {
    super(graphHeadModFunc, vertexModFunc, edgeModFunc);
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection) {
    // the resulting logical graph holds multiple graph heads
    LogicalGraph<G, V, E> modifiedGraph = executeInternal(
      collection.getGraphHeads(),
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig());

    return GraphCollection.fromDataSets(modifiedGraph.getGraphHead(),
      modifiedGraph.getVertices(),
      modifiedGraph.getEdges(),
      modifiedGraph.getConfig());
  }
}
