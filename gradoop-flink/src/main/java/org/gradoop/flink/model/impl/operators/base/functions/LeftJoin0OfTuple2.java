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

package org.gradoop.flink.model.impl.operators.base.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Left join, return first value of Tuple2.
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class LeftJoin0OfTuple2<L, R> implements
  JoinFunction<Tuple2<L, GradoopId>, R, L> {

  @Override
  public L join(Tuple2<L, GradoopId> tuple, R r) throws Exception {
    return tuple.f0;
  }
}
