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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning;

/**
 * A binary node takes two data sets containing embeddings as input which are used to compute
 * a new data set of embeddings based on the specific node implementation.
 */
public interface BinaryNode extends PlanNode {

  /**
   * Returns the left input node.
   *
   * @return left input node
   */
  PlanNode getLeftChild();

  /**
   * Returns the right input node.
   *
   * @return right input node
   */
  PlanNode getRightChild();
}
