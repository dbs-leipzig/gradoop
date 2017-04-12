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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Implements the edge difference for quads using the join as a primitive function
 */
@FunctionAnnotation.ForwardedFieldsFirst("* -> *")
public class HexapletEdgeDifference implements
  FlatJoinFunction<Hexaplet, Tuple2<GradoopId, GradoopId>, Hexaplet> {
  @Override
  public void join(Hexaplet first,
                   Tuple2<GradoopId, GradoopId> second,
                   Collector<Hexaplet> out) throws Exception {
    if (second == null) {
      out.collect(first);
    }
  }
}
