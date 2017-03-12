package org.gradoop.flink.model.impl.nested.datastructures.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

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


/**
 * left, right => left
 *
 * @param <L> left and right type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class LeftSideIfNotNull<L> implements JoinFunction<L, L, L>, CrossFunction<L, L, L> {

  @Override
  public L cross(L left, L right) throws Exception {
    return left != null ? left : right;
  }

  @Override
  public L join(L first, L second) throws Exception {
    return first != null ? first : second;
  }
}
