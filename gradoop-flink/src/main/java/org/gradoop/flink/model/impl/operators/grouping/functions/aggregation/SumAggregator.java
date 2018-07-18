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

import java.math.BigDecimal;

/**
 * Used to aggregate property values into their sum.
 */
public class SumAggregator extends PropertyValueAggregator {
  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Internal aggregate value.
   */
  private Number aggregate;

  /**
   * Creates a new aggregator
   *
   * @param propertyKey           property key to access values
   * @param aggregatePropertyKey  property key for final aggregate value
   */
  public SumAggregator(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  @Override
  protected boolean isInitialized() {
    return aggregate != null;
  }

  @Override
  protected void initializeAggregate(PropertyValue value) {
    Class<?> clazz = value.getType();
    if (clazz == Integer.class) {
      aggregate = 0;
    } else if (clazz == Long.class) {
      aggregate = 0L;
    } else if (clazz == Float.class) {
      aggregate = 0F;
    } else if (clazz == Double.class) {
      aggregate = .0;
    } else if (clazz == BigDecimal.class) {
      aggregate = new BigDecimal(0);
    } else {
      throw new IllegalArgumentException(
        "Class " + clazz + " not supported in sum aggregation");
    }
  }

  @Override
  protected void aggregateInternal(PropertyValue value) {
    if (value.isInt()) {
      aggregate = (Integer) aggregate + value.getInt();
    } else if (getAggregate().isLong() && value.isLong()) {
      aggregate = (Long) aggregate + value.getLong();
    } else if (getAggregate().isFloat() && value.isFloat()) {
      aggregate = (Float) aggregate + value.getFloat();
    } else if (getAggregate().isDouble() && value.isDouble()) {
      aggregate = (Double) aggregate + value.getDouble();
    } else if (getAggregate().isBigDecimal() && value.isBigDecimal()) {
      aggregate = ((BigDecimal) aggregate).add(value.getBigDecimal());
    } else {
      throw new IllegalArgumentException(
        "Value types do not match or are not supported.");
    }
  }

  @Override
  protected PropertyValue getAggregateInternal() {
    return PropertyValue.create(aggregate);
  }

  @Override
  public void resetAggregate() {
    aggregate = null;
  }
}
