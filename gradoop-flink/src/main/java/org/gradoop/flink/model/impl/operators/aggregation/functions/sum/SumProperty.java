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
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

import java.util.Objects;

/**
 * Superclass of aggregate functions that sum property values of elements.
 */
public class SumProperty extends BaseAggregateFunction implements Sum {

  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a SumProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   */
  public SumProperty(String propertyKey) {
    this(propertyKey, "sum_" + propertyKey);
  }

  /**
   * Creates a new instance of a SumProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  public SumProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    Objects.requireNonNull(propertyKey);
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getPropertyValue(propertyKey);
  }
}
