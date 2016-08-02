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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Aggregator that takes {@link PropertyValue} as input for aggregation.
 */
public abstract class PropertyValueAggregator
  extends BaseAggregator<PropertyValue> {

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


