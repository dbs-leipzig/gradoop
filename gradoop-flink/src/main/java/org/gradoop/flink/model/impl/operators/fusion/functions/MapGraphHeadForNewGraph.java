/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
