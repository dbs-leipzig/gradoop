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

package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.ApplyAggregateFunction;

/**
 * Aggregation function for which a default value can be
 * specified in the constructor.
 */
public abstract class AggregateWithDefaultValueFunction
  implements AggregateFunction,  ApplyAggregateFunction {

  /**
   * User defined default value.
   */
  protected Number defaultValue;

  /**
   * Constructor
   * @param defaultValue user defined default value
   */
  public AggregateWithDefaultValueFunction(Number defaultValue) {
    this.defaultValue = defaultValue;
  }

  /**
   * Return default property value.
   * @return default property value of this aggregation function
   */
  @Override
  public Number getDefaultValue() {
    return this.defaultValue;
  }

}
