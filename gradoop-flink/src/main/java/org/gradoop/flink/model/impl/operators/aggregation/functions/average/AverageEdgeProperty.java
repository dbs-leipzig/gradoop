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
package org.gradoop.flink.model.impl.operators.aggregation.functions.average;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Calculates the average of a numeric property value of all edges.
 *
 * @see AverageProperty
 */
public class AverageEdgeProperty extends AverageProperty implements EdgeAggregateFunction {

  /**
   * Create an instance of this average aggregate function with a default aggregate property key.
   * The key will be the original key, prefixed with {@code avg_}.
   *
   * @param propertyKey Key of the property to aggregate.
   */
  public AverageEdgeProperty(String propertyKey) {
    super(propertyKey);
  }

  /**
   * Create an instance of this average aggregate function.
   *
   * @param propertyKey          Key of the property to aggregate.
   * @param aggregatePropertyKey Key used to store the aggregate.
   */
  public AverageEdgeProperty(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }
}
