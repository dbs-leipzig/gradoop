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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.physical_operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;

/**
 * Creates the Cartesian Product of two embeddings.
 */
public class CartesianProduct implements PhysicalOperator{

  private final DataSet<Embedding> lhs;
  private final DataSet<Embedding> rhs;

  /**
   * New Cartesian Product Operator
   *
   * @param lhs the left hand side embedding
   * @param rhs the right hand side embedding
   */
  public CartesianProduct(DataSet<Embedding> lhs, DataSet<Embedding> rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return null;
  }
}
