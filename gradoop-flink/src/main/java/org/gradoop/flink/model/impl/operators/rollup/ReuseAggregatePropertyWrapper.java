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
package org.gradoop.flink.model.impl.operators.rollup;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Wrapper to reuse existing aggregate properties as increment.
 */
class ReuseAggregatePropertyWrapper implements AggregateFunction {
  /**
   * Wrapped aggregate function.
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Wrappes the aggregate function.
   *
   * @param aggregateFunction aggregate function
   */
  ReuseAggregatePropertyWrapper(AggregateFunction aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return aggregateFunction.aggregate(aggregate, increment);
  }

  @Override
  public String getAggregatePropertyKey() {
    return aggregateFunction.getAggregatePropertyKey();
  }

  /**
   * {@inheritDoc}
   * Reuses an existing aggregate property as increment.
   * This can be used to continue incomplete groupings.
   *
   * @param element element storing the existing aggregate property
   * @return aggregate property, may be NULL, which is handled in the operator
   */
  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getPropertyValue(getAggregatePropertyKey());
  }
}
