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

package org.gradoop.flink.model.impl.nested.operators;

import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;

/**
 * Defines a generic operator.
 *
 * Please note that within this nested model, a unary operation
 * is an operation that is either over one graph or over a collection of graphs while a binary
 * operator is an operator over two distinct graphs or over two distinct graph collections
 */
public abstract class Op {

  /**
   * DataLake that undergoes the updates
   */
  protected DataLake mother;

  /**
   * Setting the mother. This method is accessible only to the main interface
   * @param toSet   Element to be updated
   * @return        The called object (this)
   */
  public <X extends Op> X setDataLake(DataLake toSet) {
    this.mother = toSet;
    return (X)this;
  }

  /**
   * Implementation of the actual function
   * @param lake      Update the data values
   * @param left      Left argument
   * @param right     Right argument
   * @return          Result as a graph with just ids. The Data Lake is updated either with
   *                  new edges or new vertices
   */
  protected abstract IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase left,
    IdGraphDatabase right);

  /**
   * Defines the operation's name
   * @return  Returns the operation's name
   */
  public abstract String getName();

}
