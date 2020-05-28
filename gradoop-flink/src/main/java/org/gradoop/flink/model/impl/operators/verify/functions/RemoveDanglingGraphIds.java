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
package org.gradoop.flink.model.impl.operators.verify.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Remove graph ids not contained in the graph id set.
 *
 * @param <E> graph element type
 */
public class RemoveDanglingGraphIds<E extends GraphElement> extends RichMapFunction<E, E> {

  /**
   * String used to identify the graph id set on broadcast.
   */
  public static final String GRAPH_ID_SET = "graphIds";

  /**
   * Graph id set.
   */
  private GradoopIdSet idSet;

  @Override
  public void open(Configuration parameters) throws Exception {
    idSet = GradoopIdSet.fromExisting(getRuntimeContext().getBroadcastVariable(GRAPH_ID_SET));
  }

  @Override
  public E map(E element) {
    GradoopIdSet graphIds = element.getGraphIds();
    graphIds.retainAll(idSet);
    element.setGraphIds(graphIds);
    return element;
  }
}
