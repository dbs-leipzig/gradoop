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
package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * MapFunction, that writes a given property value to the GraphHead.
 */
public class WritePropertyToGraphHeadMap implements MapFunction<GraphHead, GraphHead> {

  /**
   * PropertyKey to access the value
   */
  private final String propertyKey;

  /**
   * PropertyValue to store in GraphHead
   */
  private final PropertyValue propertyValue;

  /**
   * Creates an instance of a MapFunction, that writes a given property value to the GraphHead.
   *
   * @param propertyKey PropertyKey to access the value
   * @param propertyValue PropertyValue to store in GraphHead
   */
  public WritePropertyToGraphHeadMap(String propertyKey, PropertyValue propertyValue) {
    this.propertyKey = propertyKey;
    this.propertyValue = propertyValue;
  }

  @Override
  public GraphHead map(GraphHead graphHead) throws Exception {
    graphHead.setProperty(propertyKey, propertyValue);
    return graphHead;
  }
}
