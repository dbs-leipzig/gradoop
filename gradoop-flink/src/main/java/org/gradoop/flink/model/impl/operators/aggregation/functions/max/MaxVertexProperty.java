/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate function returning the maximum of a specified property over all vertices.
 */
public class MaxVertexProperty extends MaxProperty implements VertexAggregateFunction {

  /**
   * Creates a new instance of a MaxVertexProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   */
  public MaxVertexProperty(String propertyKey) {
    super(propertyKey);
  }

  /**
   * Creates a new instance of a MaxVertexProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  public MaxVertexProperty(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }
}
