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

package org.gradoop.model.api.operators;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;

/**
 * Creates a {@link GraphCollection} based on one {@link LogicalGraph}.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public interface UnaryGraphToCollectionOperator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends Operator {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph);
}
