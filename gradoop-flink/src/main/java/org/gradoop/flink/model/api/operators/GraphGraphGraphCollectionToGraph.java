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

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.reduce.ReduceVertexFusion;

/**
 * Creates a {@link LogicalGraph} based on two input graphs and a {@link org.gradoop.flink.model.impl.GraphCollection}
 *
 * @see ReduceVertexFusion
 */
public interface GraphGraphGraphCollectionToGraph extends Operator {

  /**
   * Combining two logical graphs and a collection of graphs, we return a single logical graph
   *
   * @param left          Left operand
   * @param right         Right operand
   * @param hypervertices Graph collection as an input for both operands
   * @return              The combination of the three data
   */
  LogicalGraph execute(LogicalGraph left, LogicalGraph right, GraphCollection hypervertices);

}
