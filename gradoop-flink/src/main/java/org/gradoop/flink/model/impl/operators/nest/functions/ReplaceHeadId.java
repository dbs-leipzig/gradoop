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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Replaces the head id
 */
public class ReplaceHeadId implements
  JoinFunction<GraphHead, Tuple2<GradoopId, GradoopId>, GraphHead>,
  CrossFunction<GraphHead, Tuple2<GradoopId, GradoopId>, GraphHead> {

  @Override
  public GraphHead join(GraphHead graphHead,
    Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    graphHead.setId(gradoopIdGradoopIdTuple2.f0);
    return graphHead;
  }

  @Override
  public GraphHead cross(GraphHead graphHead,
    Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    graphHead.setId(gradoopIdGradoopIdTuple2.f0);
    return graphHead;
  }
}
