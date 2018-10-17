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
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;

/**
 * Superclass of counting aggregate functions.
 *
 * @param <T> element type
 */
public abstract class BaseCount<T extends Element> extends BaseAggregateFunction<T>
  implements Sum<T>, AggregateDefaultValue {

  /**
   * Creates a new instance of a BaseCount aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public BaseCount(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }

  @Override
  public PropertyValue getIncrement(T element) {
    return PropertyValue.create(1L);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.create(0L);
  }
}
