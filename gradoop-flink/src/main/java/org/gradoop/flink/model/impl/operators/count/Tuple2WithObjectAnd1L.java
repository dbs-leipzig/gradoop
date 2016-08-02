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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (value) => (value, 1)
 *
 * @param <T> value type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class Tuple2WithObjectAnd1L<T>
  implements MapFunction<T, Tuple2<T, Long>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<T, Long> reuseTuple;

  /**
   * Constructor
   */
  public Tuple2WithObjectAnd1L() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = 1L;
  }

  @Override
  public Tuple2<T, Long> map(T t) throws Exception {
    reuseTuple.f0 = t;
    return reuseTuple;
  }
}
