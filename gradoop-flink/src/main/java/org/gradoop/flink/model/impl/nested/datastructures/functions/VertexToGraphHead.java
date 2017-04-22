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

package org.gradoop.flink.model.impl.nested.datastructures.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Converts the vertex into a GraphHead
 */
public class VertexToGraphHead implements JoinFunction<GradoopId, Vertex, GraphHead> {

  /**
   * Reusable element
   */
  private final GraphHead reusable;

  /**
   * Default constructor
   */
  public VertexToGraphHead() {
    reusable = new GraphHead();
  }

  @Override
  public GraphHead join(GradoopId first, Vertex second) throws Exception {
    reusable.setId(first);
    if (second != null) {
      reusable.setLabel(second.getLabel());
      reusable.setProperties(second.getProperties());
    }
    return reusable;
  }
}
