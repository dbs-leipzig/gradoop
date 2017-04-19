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

package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Returns a tuple which contains the source id and target id of an edge or a tuple which contains
 * the target id and the source id of the same edge.
 */
public class VertexIdsFromEdge
  implements MapFunction<Edge, Tuple2<GradoopId, GradoopId>> {

  /**
   * False, if tuple contains of source id and target id. True if tuple contains of target id and
   * source id.
   */
  private boolean switched;

  /**
   * Constructor which initiates a mapping to tuple of source id and target id.
   */
  public VertexIdsFromEdge() {
    this(false);
  }

  /**
   * Valued constructor.
   *
   * @param switched false for tuple of source id and target id, true for vice versa
   */
  public VertexIdsFromEdge(boolean switched) {
    this.switched = switched;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, GradoopId> map(Edge edge) throws Exception {
    if (switched) {
      return new Tuple2<>(edge.getTargetId(), edge.getSourceId());
    }
    return new Tuple2<>(edge.getSourceId(), edge.getTargetId());
  }
}
