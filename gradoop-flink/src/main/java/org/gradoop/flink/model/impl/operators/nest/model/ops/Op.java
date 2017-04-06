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

package org.gradoop.flink.model.impl.operators.nest.model.ops;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;

/**
 * Defines a generic operator.
 *
 * Please note that within this nested model, a unary operation
 * is an operation that is either over one graph or over a collection of graphs while a binary
 * operator is an operator over two distinct graphs or over two distinct graph collections
 *
 * @param <Left> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the left operand
 * @param <Right> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the right operand
 * @param <Res> Elements extending the NestedIndexing and supporting the previous state of the
 *            computation for the result
 */
public abstract class Op<Left extends NestingIndex,
  Right extends NestingIndex,
  Res extends NestingIndex> {


  /**
   * DataLake that undergoes the updates
   */
  protected LogicalGraph mother;

  /**
   * Setting the mother. This method is accessible only to the main interface
   * @param toSet   Element to be updated
   * @return        The called object (this)
   * @param <X>     Returns this
   */
  public <X extends Op> X setDataLake(LogicalGraph toSet) {
    this.mother = toSet;
    return (X) this;
  }


  /**
   * Implementation of the actual function
   * @param flat      The normalized graph containing the ground truth.
   * @param left      Left argument
   * @param right     Right argument
   * @return          Result as a graph with just ids. The Data Lake is updated either with
   *                  new edges or new vertices
   */
  protected abstract Res runWithArgAndLake(LogicalGraph flat, Left left, Right right);

  /**
   * Defines the operation's name
   * @return  Returns the operation's name
   */
  public abstract String getName();

}
