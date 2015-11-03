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

import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Creates a {@link LogicalGraph} based on two input graphs.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see org.gradoop.model.impl.operators.logicalgraph.binary.Combination
 * @see org.gradoop.model.impl.operators.logicalgraph.binary.Exclusion
 * @see org.gradoop.model.impl.operators.logicalgraph.binary.Overlap
 */
public interface BinaryGraphToGraphOperator<VD extends EPGMVertex, ED extends EPGMEdge, GD extends EPGMGraphHead> extends
  Operator {
  /**
   * Executes the operator.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return operator result
   */
  LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> firstGraph,
    LogicalGraph<VD, ED, GD> secondGraph);
}
