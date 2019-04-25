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

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

import java.util.Arrays;
import java.util.Objects;

/**
 * Base class for aggregate functions determining the average of a numeric property value.
 */
public class AverageProperty extends BaseAggregateFunction implements Average {

  /**
   * A property value containing the number {@code 1}, as a {@code long}.
   */
  private static final PropertyValue ONE = PropertyValue.create(1L);

  /**
   * The key used to read the value to aggregate from.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a base average aggregate function with a default aggregate property
   * key (will be the original property key with prefix {@code avg_}).
   *
   * @param propertyKey The key of the property to aggregate.
   */
  public AverageProperty(String propertyKey) {
    this(propertyKey, "avg_" + propertyKey);
  }

  /**
   * Creates a new instance of a base average aggregate function.
   *
   * @param propertyKey          The key of the property to aggregate.
   * @param aggregatePropertyKey The propertyKey used to store the aggregate.
   */
  public AverageProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    this.propertyKey = Objects.requireNonNull(propertyKey);
  }

  @Override
  public PropertyValue getIncrement(EPGMElement element) {
    PropertyValue value = element.getPropertyValue(propertyKey);
    if (value == null) {
      return Average.IGNORED_VALUE;
    } else if (!value.isNumber()) {
      throw new IllegalArgumentException("Property value has to be a number.");
    } else {
      return PropertyValue.create(Arrays.asList(value, ONE));
    }
  }
}
