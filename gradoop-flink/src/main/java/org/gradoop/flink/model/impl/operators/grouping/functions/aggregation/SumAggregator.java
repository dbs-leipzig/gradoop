/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
