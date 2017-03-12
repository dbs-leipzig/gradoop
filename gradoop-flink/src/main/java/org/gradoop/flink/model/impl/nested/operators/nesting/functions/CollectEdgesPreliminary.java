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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * Converting a Quad representing an Edge into a preliminary representation, that
 * could be used either for representing IdGraphDatabase edges or for defining
 * which edges are to be copied from the original ones within the data lake
 */
@FunctionAnnotation.ForwardedFields("f4 -> f0; f0 -> f1")
public class CollectEdgesPreliminary implements MapFunction<Hexaplet, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId, GradoopId> reusable;

  /**
   * Default constructor
   */
  public CollectEdgesPreliminary() {
    reusable = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(Hexaplet value) throws Exception {
    reusable.f0 = value.f4;
    reusable.f1 = value.f0;
    return reusable;
  }
}

