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
 * Changes each head from the graph
 */
@FunctionAnnotation.ForwardedFields("f1 -> f1")
public class SetEdgesToNewGraph implements
  MapFunction<Tuple2<GradoopId, GradoopId>,Tuple2<GradoopId, GradoopId>> {

  /**
   * New id for the header
   */
  private final GradoopId id;

  /**
   * Updates the header of each head by associating each edge to the new graph
   * @param id    new id to be set
   */
  public SetEdgesToNewGraph(GradoopId id) {
    this.id = id;
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(
    Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    gradoopIdGradoopIdTuple2.f0 = id;
    return gradoopIdGradoopIdTuple2;
  }

}
