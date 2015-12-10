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

package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Transform an edge into a Tuple3 of edge id, source vertex and
 * target id
 *
 * @param <E> EPGM edge type
 */
public class EdgeToTuple<E extends EPGMEdge>
  implements MapFunction<E, Tuple3<E, GradoopId, GradoopId>> {
  @Override
  public Tuple3<E, GradoopId, GradoopId> map(E edge) {
    return new Tuple3<>(edge, edge.getSourceId(), edge.getTargetId());
  }
}
