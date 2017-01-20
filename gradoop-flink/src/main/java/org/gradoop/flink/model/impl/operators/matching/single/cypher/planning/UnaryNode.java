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

import java.util.Objects;

/**
 * A unary node takes a single data set containing embeddings as input which is used to compute
 * a new data set of embeddings based on the specific node implementation.
 */
public abstract class UnaryNode extends PlanNode {
  /**
   * Represents the input of that node.
   */
  private final PlanNode childNode;

  /**
   * Creates a new unary node
   *
   * @param childNode input of the node
   */
  public UnaryNode(PlanNode childNode) {
    Objects.requireNonNull(childNode);
    this.childNode = childNode;
  }

  /**
   * Returns the child (input) node of that node.
   *
   * @return child plan node
   */
  public PlanNode getChildNode() {
    return childNode;
  }
}
