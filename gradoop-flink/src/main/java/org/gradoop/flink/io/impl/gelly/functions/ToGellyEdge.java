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

package org.gradoop.flink.io.impl.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * MapFunction to convert a EPGVertex to a Gelly Vertex
 */
public class ToGellyEdge
  implements MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, Edge>> {

  @Override
  public org.apache.flink.graph.Edge<GradoopId, Edge> map(Edge e)
      throws Exception {
    return new org.apache.flink.graph.Edge<>(
      e.getSourceId(), e.getTargetId(), e);
  }
}
