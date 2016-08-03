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

package org.gradoop.flink.model.impl.functions.bool;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Logical "NOT" as Flink function.
 */
public class Not implements MapFunction<Boolean, Boolean> {

  @Override
  public Boolean map(Boolean b) throws Exception {
    return !b;
  }

  /**
   * Map a a boolean dataset to its inverse.
   *
   * @param b boolean dataset
   * @return inverse dataset
   */
  public static DataSet<Boolean> map(DataSet<Boolean> b) {
    return b.map(new Not());
  }
}
