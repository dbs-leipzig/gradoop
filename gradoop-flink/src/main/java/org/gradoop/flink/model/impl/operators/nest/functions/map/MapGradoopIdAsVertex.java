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

package org.gradoop.flink.model.impl.operators.nest.functions.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Creates an empty vertex from a GradoopId
 */
public class MapGradoopIdAsVertex implements MapFunction<GradoopId, Vertex>  {

  /**
   * Reusable element
   */
  private final Vertex reusable;

  /**
   * Reusable list
   */
  private final GradoopIdList list;

  /**
   * Default constructor
   */
  public MapGradoopIdAsVertex() {
    reusable = new Vertex();
    list = new GradoopIdList();
  }

  @Override
  public Vertex map(GradoopId value) throws Exception {
    reusable.setId(value);
    list.clear();
    list.add(value);
    reusable.setGraphIds(list);
    return reusable;
  }
}
