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
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Adds the given graph head identifier to the graph element.
 *
 * @param <GE> EPGM graph element
 */
public class AddToGraph<GE extends GraphElement> implements
  MapFunction<GE, GE> {

  /**
   * Graph head identifier which gets added to the graph element.
   */
  private final GradoopId graphHeadId;

  /**
   * Creates a new GraphContainmentUpdater
   *
   * @param graphHead graph head used for updating
   */
  public AddToGraph(GraphHead graphHead) {
    this.graphHeadId = graphHead.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GE map(GE graphElement) throws Exception {
    graphElement.addGraphId(graphHeadId);
    return graphElement;
  }
}
