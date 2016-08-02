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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Adds the given graph head identifier to the graph element. The identifier
 * is transferred via broadcasting.
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class AddToGraphBroadcast
  <GE extends GraphElement>
  extends RichMapFunction<GE, GE> {

  /**
   * constant string for "graph id"
   */
  public static final String GRAPH_ID = "graphId";

  /**
   * Graph head identifier which gets added to the graph element.
   */
  private GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphId = getRuntimeContext()
      .<GradoopId>getBroadcastVariable(GRAPH_ID).get(0);
  }

  @Override
  public GE map(GE graphElement) throws Exception {
    graphElement.addGraphId(graphId);
    return graphElement;
  }
}
