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

import org.gradoop.flink.model.impl.operators.nest2.model.indices.NestedIndexing;

/**
 * Defines a binary operator
 *
 * @param <Left> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the left operand
 * @param <Right> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the right operand
 * @param <Res> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the result
 */
public abstract class BinaryOp<Left extends NestedIndexing,
  Right extends NestedIndexing,
  Res extends NestedIndexing> extends Op<Left,Right,Res> {

  /**
   * Public access to the inxternal operation. The data lake is not exposed
   * @param left    Left argument
   * @param right   Right argument
   * @return        Result as a graph with just ids. The DataLake, representing the computation
   *                state, is updated with either new vertices or new edges
   */
  public Res with(Left left, Right right) {
    return runWithArgAndLake(mother, left, right);
  }

}
