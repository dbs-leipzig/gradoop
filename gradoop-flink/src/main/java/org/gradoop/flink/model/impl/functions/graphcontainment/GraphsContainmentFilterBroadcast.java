/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * Superclass of multi graph containment filters using broadcast variables.
 *
 * @param <GE> graph element type
 */
public abstract class GraphsContainmentFilterBroadcast
  <GE extends GraphElement> extends RichFilterFunction<GE> {

  /**
   * constant string for "graph ids"
   */
  public static final String GRAPH_IDS = "graphIds";

  /**
   * graph ids
   */
  protected Collection<GradoopId> graphIds;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    graphIds = getRuntimeContext().getBroadcastVariable(GRAPH_IDS);
  }
}
