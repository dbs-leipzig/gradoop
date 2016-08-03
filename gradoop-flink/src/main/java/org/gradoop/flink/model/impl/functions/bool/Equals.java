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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Equality as Flink function.
 *
 * @param <T> input element type
 */
public class Equals<T>
  implements CrossFunction<T, T, Boolean> {

  @Override
  public Boolean cross(T left, T right) throws Exception {
    return left.equals(right);
  }

  /**
   * Checks for pair-wise equality between the elements of the given input sets.
   *
   * @param first   first input dataset
   * @param second  second input dataset
   * @param <T>     dataset element type
   * @return dataset with {@code boolean} values for each pair
   */
  public static <T> DataSet<Boolean> cross(
    DataSet<T> first, DataSet<T> second) {
    return first.cross(second).with(new Equals<T>());
  }
}
