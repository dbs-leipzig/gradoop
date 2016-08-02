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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => f1
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
@FunctionAnnotation.ForwardedFields("f1->*")
public class Value1Of3<T0, T1, T2>
  implements
  MapFunction<Tuple3<T0, T1, T2>, T1>, KeySelector<Tuple3<T0, T1, T2>, T1> {

  @Override
  public T1 map(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f1;
  }

  @Override
  public T1 getKey(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f1;
  }
}
