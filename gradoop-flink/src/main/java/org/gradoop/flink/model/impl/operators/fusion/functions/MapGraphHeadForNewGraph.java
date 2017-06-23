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

package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Creates a new head and sets a new graph id
 */
public class MapGraphHeadForNewGraph implements MapFunction<GraphHead, GraphHead> {

  /**
   * Id to be setted
   */
  private final GradoopId graphId;

  /**
   * Default constructor
   * @param newGraphid   id to be setted
   */
  public MapGraphHeadForNewGraph(GradoopId newGraphid) {
    graphId = newGraphid;
  }

  @Override
  public GraphHead map(GraphHead value) throws Exception {
    value.setId(graphId);
    return value;
  }
}
