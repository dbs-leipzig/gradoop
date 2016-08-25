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

/**
 * Special sum aggregator to count elements.
 */
public class CountAggregator extends SumAggregator {

  /**
   * Default property key to fetch values for aggregation.
   */
  public static final String DEFAULT_PROPERTY_KEY = "*";

  /**
   * Default property key to store the result of the aggregate function.
   */
  public static final String DEFAULT_AGGREGATE_PROPERTY_KEY = "count";

  /**
   * Aggregate value to count the number of calls of {@link #aggregate(Object)}.
   */
  private Long aggregate;

  /**
   * Creates a new count aggregator
   */
  public CountAggregator() {
    this(DEFAULT_PROPERTY_KEY, DEFAULT_AGGREGATE_PROPERTY_KEY);
  }

  /**
   * Creates a new count aggregator
   *
   * @param aggregatePropertyKey used to store the final aggregate value
   */
  public CountAggregator(String aggregatePropertyKey) {
    this(DEFAULT_PROPERTY_KEY, aggregatePropertyKey);
  }

  /**
   * Creates a new count aggregator
   *
   * @param propertyKey           used to define the property to aggregate
   * @param aggregatePropertyKey  used to store the final aggregate value
   */
  public CountAggregator(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
    aggregate = 0L;
  }

  @Override
  public void aggregate(PropertyValue value) {
    aggregateInternal(value);
  }

  @Override
  protected boolean isInitialized() {
    return true;
  }

  @Override
  protected void initializeAggregate(PropertyValue value) {
    aggregate = 0L;
  }

  @Override
  protected void aggregateInternal(PropertyValue value) {
    aggregate += value.getLong();
  }

  @Override
  protected PropertyValue getAggregateInternal() {
    return PropertyValue.create(aggregate);
  }

  @Override
  public void resetAggregate() {
    aggregate = 0L;
  }
}
