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

package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Transform an edge into a Tuple3.
 *
 * f0: edge identifier
 * f1: source vertex identifier
 * f2: target vertex identifier
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("*->f0;sourceId->f1;targetId->f2")
public class EdgeToTriple<E extends Edge>
  implements MapFunction<E, Tuple3<E, GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple3<E, GradoopId, GradoopId> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<E, GradoopId, GradoopId> map(E edge) {
    reuseTuple.setFields(edge, edge.getSourceId(), edge.getTargetId());
    return reuseTuple;
  }
}
