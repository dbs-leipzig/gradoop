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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Convert an edge coming from "ColelctEdgesPreliminary" into a final
 * IdGraphDatabase representation.
 */
@FunctionAnnotation.ForwardedFields("f0 -> f1")
public class CollectEdges implements
  FlatMapFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable tuple to be returned
   */
  private final Tuple2<GradoopId,GradoopId> reusable;

  /**
   * Check if the
   */
  private final boolean includeNotUpdatedEdges;

  public CollectEdges(GradoopId newGraphId, boolean includeNotUpdatedEdges) {
    reusable = new Tuple2<>();
    reusable.f0 = newGraphId;
    this.includeNotUpdatedEdges = includeNotUpdatedEdges;
  }

  @Override
  public void flatMap(Tuple2<GradoopId, GradoopId> x,
    Collector<Tuple2<GradoopId, GradoopId>> out) throws Exception {
    if (includeNotUpdatedEdges) {
      if (!x.f0.equals(GradoopId.NULL_VALUE)) {
        reusable.f1 = x.f0;
      } else {
        reusable.f1 = x.f1;
      }
    }
    if (includeNotUpdatedEdges || (!x.f0.equals(GradoopId.NULL_VALUE))) {
      out.collect(reusable);
    }
  }
}

