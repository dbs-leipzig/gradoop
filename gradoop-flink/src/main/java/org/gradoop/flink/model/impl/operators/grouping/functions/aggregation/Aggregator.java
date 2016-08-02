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

import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;

/**
 * Defines an aggregate function that can be applied on vertex and edge groups
 * during {@link Grouping}.
 *
 * @param <IN> input type for the specific aggregation function
 */
public interface Aggregator<IN> extends Serializable {
  /**
   * Adds the given value to the aggregate.
   *
   * @param value value to aggregate
   */
  void aggregate(IN value);
  /**
   * Returns the final aggregate.
   *
   * @return aggregate
   */
  PropertyValue getAggregate();
  /**
   * Returns the key of the property which is being aggregated (e.g. age, price)
   *
   * @return property key of the value to be aggregated
   */
  String getPropertyKey();
  /**
   * Returns the property key, which is used to store the final aggregate value
   * (e.g. COUNT, AVG(age), ...)
   *
   * @return property key to store the resulting aggregate value
   */
  String getAggregatePropertyKey();
  /**
   * Resets the internal aggregate value.
   */
  void resetAggregate();
}
