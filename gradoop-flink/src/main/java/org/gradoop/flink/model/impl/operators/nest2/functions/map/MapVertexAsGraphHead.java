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

package org.gradoop.flink.model.impl.operators.nest2.functions.map;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Maps a vertex back to a GraphHead
 */
public class MapVertexAsGraphHead implements MapFunction<Vertex, GraphHead>,
  FlatJoinFunction<Vertex, GraphHead, GraphHead> {

  /**
   * Reusable element
   */
  private final GraphHead reusable;

  /**
   * Default constructor
   */
  public MapVertexAsGraphHead() {
    reusable = new GraphHead();
  }

  @Override
  public GraphHead map(Vertex value) throws Exception {
    reusable.setId(value.getId());
    reusable.setLabel(value.getLabel());
    reusable.setProperties(value.getProperties());
    return reusable;
  }

  @Override
  public void join(Vertex first, GraphHead second, Collector<GraphHead> out) throws Exception {
    if (second == null) {
      out.collect(map(first));
    }
  }
}
