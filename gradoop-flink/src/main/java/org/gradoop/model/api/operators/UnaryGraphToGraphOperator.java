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

package org.gradoop.model.api.operators;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.model.LogicalGraph;

/**
 * Creates a {@link LogicalGraph} based on one input {@link LogicalGraph}.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public interface UnaryGraphToGraphOperator<VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead> extends
  Operator {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   * @throws Exception
   */
  LogicalGraph<GD, VD, ED> execute(LogicalGraph<GD, VD, ED> graph) throws
    Exception;
}
