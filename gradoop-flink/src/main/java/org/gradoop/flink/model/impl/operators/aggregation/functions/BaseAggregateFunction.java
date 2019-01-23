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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Objects;

/**
 * Base implementation of AggregateFunction providing a custom aggregate property key.
 */
public abstract class BaseAggregateFunction implements AggregateFunction {
  /**
   * Key of the aggregate property.
   */
  private String aggregatePropertyKey;

  /**
   * Creates a new instance of a base aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public BaseAggregateFunction(String aggregatePropertyKey) {
    setAggregatePropertyKey(aggregatePropertyKey);
  }

  /**
   * Sets the property key used to store the aggregate value.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public void setAggregatePropertyKey(String aggregatePropertyKey) {
    Objects.requireNonNull(aggregatePropertyKey);
    this.aggregatePropertyKey = aggregatePropertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return aggregatePropertyKey;
  }
}
