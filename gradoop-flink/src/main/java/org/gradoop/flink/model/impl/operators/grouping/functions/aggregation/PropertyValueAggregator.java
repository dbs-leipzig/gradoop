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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Aggregator that takes {@link PropertyValue} as input for aggregation.
 */
public abstract class PropertyValueAggregator extends BaseAggregator<PropertyValue> {

  /**
   * Creates a new aggregator
   *
   * @param propertyKey           used to fetch property value from elements
   * @param aggregatePropertyKey  used to store the final aggregate value
   */
  protected PropertyValueAggregator(String propertyKey,
    String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  @Override
  public void aggregate(PropertyValue value) {
    value = checkNotNull(value);
    if (!value.isNull()) {
      if (!isInitialized()) {
        initializeAggregate(value);
      }
      aggregateInternal(value);
    }
  }
}


