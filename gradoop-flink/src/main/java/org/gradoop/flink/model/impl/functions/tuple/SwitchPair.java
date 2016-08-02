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

/**
 * Switches the fields of a Flink pair
 * @param <A> type of field 0
 * @param <B> type of field 1
 */
@FunctionAnnotation.ForwardedFields("f0->f1;f1->f0")
public class SwitchPair<A, B>
  implements MapFunction<Tuple2<A, B>, Tuple2<B, A>> {

  @Override
  public Tuple2<B, A> map(Tuple2<A, B> pair) throws Exception {
    return new Tuple2<>(pair.f1, pair.f0);
  }
}
