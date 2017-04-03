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

package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * (object,count1),(object,count2) -> (object,count1 + count2)
 *
 * @param <T> object type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class SumCounts<T> implements JoinFunction<WithCount<T>, WithCount<T>, WithCount<T>> {

  @Override
  public WithCount<T> join(WithCount<T> first, WithCount<T> second) throws Exception {
    first.setCount(first.getCount() + second.getCount());
    return first;
  }
}
