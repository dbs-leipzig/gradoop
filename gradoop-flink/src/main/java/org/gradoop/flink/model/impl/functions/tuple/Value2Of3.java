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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => f2
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
public class Value2Of3<T0, T1, T2>
  implements MapFunction<Tuple3<T0, T1, T2>, T2>,
  KeySelector<Tuple3<T0, T1, T2>, T2> {

  @Override
  public T2 map(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f2;
  }

  @Override
  public T2 getKey(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f2;
  }
}
