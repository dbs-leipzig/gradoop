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
