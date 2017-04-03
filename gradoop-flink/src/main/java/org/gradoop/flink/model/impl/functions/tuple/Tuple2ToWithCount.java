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

package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object, count) -> WithCount(object, count)
 *
 * @param <T> object type
 */
@FunctionAnnotation.ForwardedFields("f0;f1")
public class Tuple2ToWithCount<T> implements MapFunction<Tuple2<T, Long>, WithCount<T>> {
  /**
   * Reduce object instantiations
   */
  private final WithCount<T> reuseTuple = new WithCount<>();

  @Override
  public WithCount<T> map(Tuple2<T, Long> value) throws Exception {
    reuseTuple.setObject(value.f0);
    reuseTuple.setCount(value.f1);
    return reuseTuple;
  }
}
