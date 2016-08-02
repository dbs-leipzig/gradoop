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

package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Duplicates a data set element k times.
 *
 * @param <T> element type
 */
public class Duplicate<T> implements FlatMapFunction<T, T> {

  /**
   * number of duplicates per input element (k)
   */
  private final int multiplicand;

  /**
   * Constructor.
   *
   * @param multiplicand number of duplicates per input element
   */
  public Duplicate(int multiplicand) {
    this.multiplicand = multiplicand;
  }

  @Override
  public void flatMap(T original, Collector<T> duplicates) throws  Exception {
    for (int i = 1; i <= multiplicand; i++) {
      duplicates.collect(original);
    }
  }
}
