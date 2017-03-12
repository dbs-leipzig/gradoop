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
 * Defines an unary operator for the ensted model
 */
public abstract class UnaryOp extends BinaryOp {

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase left,
    IdGraphDatabase right) {
    return runWithArgAndLake(lake, left);
  }

  /**
   * Operation to be overridden by the implementing user
   * @param lake  Source containing all the data information
   * @param data  A subgraph within the datalake containing only ids and the nesting information
   * @return      The result of the operation
   */
  protected abstract IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data);

  /**
   * Uses the data hiding for updating the DataLake with the outcome of the unary operator
   * @param data  A subgraph within the datalake containing only ids and the nesting information
   * @return      The result of the operation
   */
  public IdGraphDatabase with(IdGraphDatabase data) {
    return runWithArgAndLake(mother, data, null);
  }

}
