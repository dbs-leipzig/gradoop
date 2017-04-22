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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Defines a new vertex from a GraphHead
 */
public class MapGraphHeadAsVertex implements MapFunction<GraphHead, Vertex>,
  FlatJoinFunction<GraphHead, Vertex, Vertex> {

  /**
   * Reusable vertex
   */
  private final Vertex reusable;

  /**
   * List to be set inside the reusable element
   */
  private final GradoopIdList list;

  /**
   * Default constructor
   */
  public MapGraphHeadAsVertex() {
    reusable = new Vertex();
    list = new GradoopIdList();
  }

  @Override
  public Vertex map(GraphHead value) throws Exception {
    reusable.setId(value.getId());
    reusable.setLabel(value.getLabel());
    reusable.setProperties(value.getProperties());
    list.clear();
    list.add(value.getId());
    reusable.setGraphIds(list);
    return reusable;
  }

  @Override
  public void join(GraphHead first, Vertex second, Collector<Vertex> out) throws Exception {
    if (second == null) {
      out.collect(map(first));
    }
  }
}
