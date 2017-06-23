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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;

/**
 * left, right => left
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSide<L, R> implements CrossFunction<L, R, L>, JoinFunction<L, R, L>,
  CoGroupFunction<L, R, L> {

  @Override
  public L cross(L left, R right) throws Exception {
    return left;
  }

  @Override
  public L join(L first, R second) throws Exception {
    return first;
  }

  @Override
  public void coGroup(Iterable<L> first, Iterable<R> second, Collector<L> out) throws Exception {
    if (second.iterator().hasNext()) {
      for (L x : first) {
        out.collect(x);
      }
    }
  }
}
