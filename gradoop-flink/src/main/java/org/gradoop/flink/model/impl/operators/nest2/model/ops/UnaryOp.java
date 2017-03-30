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

package org.gradoop.flink.model.impl.operators.nest2.model.ops;

import org.apache.jasper.tagplugins.jstl.core.Out;
import org.gradoop.flink.model.impl.operators.nest2.model.FlatModel;
import org.gradoop.flink.model.impl.operators.nest2.model.indices.NestedIndexing;

/**
 * Defines an unary operator for the ensted model
 *
 * @param <Input> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the left operand
 * @param <Output> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the result
 */
public abstract class UnaryOp<Input extends NestedIndexing,
                              Output extends NestedIndexing>
  extends BinaryOp<Input,NestedIndexing,Output> {

  @Override
  protected Output runWithArgAndLake(FlatModel lake, Input left,
    NestedIndexing right) {
    return runWithArgAndLake(lake, left);
  }

  /**
   * Operation to be overridden by the implementing user
   * @param lake  Source containing all the data information
   * @param data  A subgraph within the datalake containing only ids and the nesting information
   * @return      The result of the operation
   */
  protected abstract Output runWithArgAndLake(FlatModel lake, Input data);

  /**
   * Uses the data hiding for updating the DataLake with the outcome of the unary operator
   * @param data  A subgraph within the datalake containing only ids and the nesting information
   * @return      The result of the operation
   */
  public NestedIndexing with(Input data) {
    return runWithArgAndLake(mother, data, null);
  }

}
