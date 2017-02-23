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

package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.EdgeReduceVertexFusion;

/**
 * Creates a {@link LogicalGraph} based on two input graphs.
 *
 * @see EdgeReduceVertexFusion
 */
public interface TernaryGraphToGraphOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @param thirdGraph third input graph
   * @return operator result
   */
  LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph, LogicalGraph thirdGraph);
}
