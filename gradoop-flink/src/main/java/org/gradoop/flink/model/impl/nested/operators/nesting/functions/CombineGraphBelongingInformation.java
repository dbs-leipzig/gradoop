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

package org.gradoop.flink.model.impl.nested.operators.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * Associates each exaplet to the nested graph where it appears
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0; f1 -> f1; f2 -> f2; f3 -> f3; f4 -> f4")
@FunctionAnnotation.ForwardedFieldsSecond("f0 -> f5")
public class CombineGraphBelongingInformation implements JoinFunction<Hexaplet, Tuple2<GradoopId,
  GradoopId>, Hexaplet>  {

  @Override
  public Hexaplet join(Hexaplet first, Tuple2<GradoopId, GradoopId> second) throws Exception {
    first.f5 = second.f0;
    return first;
  }
}
