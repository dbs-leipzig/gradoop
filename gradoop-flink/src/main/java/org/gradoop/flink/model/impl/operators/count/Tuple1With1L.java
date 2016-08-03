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

package org.gradoop.flink.model.impl.operators.count;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Maps something to numeric ONE in a tuple 1.
 *
 * @param <T> type of something
 */
public class Tuple1With1L<T>
  implements JoinFunction<T, T, Tuple1<Long>>, MapFunction<T, Tuple1<Long>> {

  /**
   * Numeric one
   */
  private static final Tuple1<Long> ONE = new Tuple1<>(1L);

  @Override
  public Tuple1<Long> join(T left, T right) throws Exception {
    return ONE;
  }

  @Override
  public Tuple1<Long> map(T x) throws Exception {
    return ONE;
  }
}

