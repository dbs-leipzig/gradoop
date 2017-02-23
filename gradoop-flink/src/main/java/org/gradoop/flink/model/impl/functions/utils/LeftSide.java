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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * left, right => left
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R> implements JoinFunction<L, R, L>, KeySelector<Tuple2<L, R>, L>,
  MapFunction<Tuple2<L, R>, L>,CrossFunction<L, R, L> {

  @Override
  public L cross(L left, R right) throws Exception {
    return left;
  }

  @Override
  public L getKey(Tuple2<L, R> value) throws Exception {
    return value.f0;
  }

  @Override
  public L map(Tuple2<L, R> value) throws Exception {
    return value.f0;
  }

  @Override
  public L join(L left, R right) throws Exception {
    return left;
  }

}
