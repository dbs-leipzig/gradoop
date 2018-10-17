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
package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.ElementAggregateFunction;

/**
 * Aggregate function returning the maximum of a specified property over all elements.
 */
public class MaxProperty extends BaseMaxProperty<Element> implements ElementAggregateFunction {

  /**
   * Creates a new instance of a MaxProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   */
  public MaxProperty(String propertyKey) {
    super(propertyKey);
  }

  /**
   * Creates a new instance of a MaxProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  public MaxProperty(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }
}
