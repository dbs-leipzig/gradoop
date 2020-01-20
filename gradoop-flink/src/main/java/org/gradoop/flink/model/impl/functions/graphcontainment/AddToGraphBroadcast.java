/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Adds the given graph head identifier to the graph element. The identifier
 * is transferred via broadcasting.
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class AddToGraphBroadcast
  <GE extends EPGMGraphElement>
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
