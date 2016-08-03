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

package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Superclass of single graph containment filters using broadcast variables.
 *
 * @param <GE> graph element type
 */
public abstract class GraphContainmentFilterBroadcast
  <GE extends GraphElement> extends RichFilterFunction<GE> {

  /**
   * constant string for "graph id"
   */
  public static final String GRAPH_ID = "graphId";

  /**
   * graph id
   */
  protected GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphId = getRuntimeContext()
      .<GradoopId>getBroadcastVariable(GRAPH_ID).get(0);
  }
}
