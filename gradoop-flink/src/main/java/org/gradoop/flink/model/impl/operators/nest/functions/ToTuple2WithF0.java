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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates an element of the stack from an head id
 */
@FunctionAnnotation.ForwardedFields("* -> f1")
public class ToTuple2WithF0 implements MapFunction<GradoopId, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable Element
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructior
   * @param nestedGraphId   Id associated to the new graph in the EPGM model
   */
  public ToTuple2WithF0(GradoopId nestedGraphId) {
    reusable = new Tuple2<>();
    reusable.f0 = nestedGraphId;
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(GradoopId gradoopId) throws Exception {
    reusable.f1 = gradoopId;
    return reusable;
  }
}
