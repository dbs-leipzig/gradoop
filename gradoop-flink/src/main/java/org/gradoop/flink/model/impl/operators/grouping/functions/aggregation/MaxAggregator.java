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
package org.gradoop.flink.model.impl.operators.grouping.functions.aggregation;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Used to find the maximum value in a set of values.
 */
public class MaxAggregator extends PropertyValueAggregator {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate value. No need to deserialize as it is just used for comparison.
   */
  private PropertyValue aggregate;

  /**
   * Creates a new aggregator
   *
   * @param propertyKey          property key to access values
   * @param aggregatePropertyKey property key for final aggregate value
   */
  public MaxAggregator(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  @Override
  protected boolean isInitialized() {
    return aggregate != null;
  }

  @Override
  protected void aggregateInternal(PropertyValue value) {
    if (value.compareTo(aggregate) > 0) {
      aggregate = value;
    }
  }

  @Override
  protected PropertyValue getAggregateInternal() {
    return aggregate;
  }

  @Override
  protected void initializeAggregate(PropertyValue value) {
    aggregate = value;
  }

  @Override
  public void resetAggregate() {
    aggregate = null;
  }
}
