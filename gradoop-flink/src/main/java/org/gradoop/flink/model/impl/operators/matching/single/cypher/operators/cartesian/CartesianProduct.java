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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.cartesian;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

/**
 * Creates the Cartesian Product of two embeddings.
 */
public class CartesianProduct implements PhysicalOperator {

  /**
   * Left hand side embeddings
   */
  private final DataSet<Embedding> left;
  /**
   * right hand side embeddings
   */
  private final DataSet<Embedding> right;
  /**
   * Cross Hint
   */
  private final CrossOperatorBase.CrossHint crossHint;

  /**
   * New Cartesian Product Operator
   *
   * @param left the left hand side embedding
   * @param right the right hand side embedding
   * @param crossHint cross hint
   */
  public CartesianProduct(DataSet<Embedding> left, DataSet<Embedding> right,
    CrossOperatorBase.CrossHint crossHint) {
    this.left = left;
    this.right = right;
    this.crossHint = crossHint;
  }

  /**
   * New Cartesian Product Operator with default cross hint
   * @param left left hand side embedding
   * @param right right hand side embedding
   */
  public CartesianProduct(DataSet<Embedding> left, DataSet<Embedding> right) {
    this(left, right, CrossOperatorBase.CrossHint.OPTIMIZER_CHOOSES);
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
