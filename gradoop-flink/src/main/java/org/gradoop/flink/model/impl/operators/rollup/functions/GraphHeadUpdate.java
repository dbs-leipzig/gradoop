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
package org.gradoop.flink.model.impl.operators.rollup.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * The GraphHeadUpdate MapFunction assigns a new label and a comma separated list of used grouping
 * keys to the graphHead.
 */
public class GraphHeadUpdate implements MapFunction<GraphHead, GraphHead> {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 42L;

  /**
   * New graphHead label.
   */
  private String label;

  /**
   * Property key used to store the grouping keys.
   */
  private String propertyKey;

  /**
   * Grouping keys to be stored.
   */
  private String groupingKeys;

  /**
   * Creates an instance of GraphHeadUpdate.
   *
   * @param label        new grapHead label
   * @param propertyKey  property key used to store the grouping keys
   * @param groupingKeys grouping keys to be stored
   */
  public GraphHeadUpdate(String label, String propertyKey, String groupingKeys) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.groupingKeys = groupingKeys;
  }

  /**
   * Updates the graphHead.
   *
   * @param graphHead  original graphHead to be updated
   * @return updated graphHead
   */
  @Override
  public GraphHead map(GraphHead graphHead) throws Exception {
    graphHead.setLabel(label);
    graphHead.setProperty(propertyKey, String.join(",", groupingKeys));

    return graphHead;
  }
}
