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
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;

/**
 * Superclass if aggregate functions that sum property values of elements.
 *
 * @param <T> element type
 */
public abstract class BaseSumProperty<T extends Element> extends BaseAggregateFunction<T>
  implements Sum<T> {

  /**
   * Property key whose value should be aggregated.
   */
  private String propertyKey;

  /**
   * Creates a new instance of a BaseSumProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   */
  BaseSumProperty(String propertyKey) {
    super("sum_" + propertyKey);
    this.propertyKey = propertyKey;
  }

  /**
   * Creates a new instance of a BaseSumProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  BaseSumProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue getIncrement(T element) {
    return element.getPropertyValue(propertyKey);
  }
}
