/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Type;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.temporal.TemporalUnit;
import java.util.Arrays;

/**
 * Calculate the average duration of a time dimension of one given {@link TimeDimension} of temporal elements.
 * Time intervals with either the start or end time set to the respective default value will be ignored.
 */
public class AverageDuration extends AbstractDurationAggregateFunction
  implements Average, TemporalAggregateFunction {

  /**
   * A property value containing the number {@code 1}, as a {@link Long}.
   */
  private static final PropertyValue ONE = PropertyValue.create(1L);

  /**
   * Creates a new instance of this aggregate function.
   *
   * @param aggregatePropertyKey the property key of the new property where the aggregated value is stored
   * @param dimension the time dimension to consider
   * @param unit the temporal unit into which the result is converted. The supported units are specified in
   *             {@link AbstractDurationAggregateFunction#SUPPORTED_UNITS}.
   */
  public AverageDuration(String aggregatePropertyKey, TimeDimension dimension, TemporalUnit unit) {
    super(aggregatePropertyKey, dimension, unit);
  }

  /**
   * Create an instance of the {@link AverageDuration} aggregate function.
   *
   * @param aggregatePropertyKey the property key of the new property where the aggregated value is stored
   * @param dimension the time dimension to consider
   */
  public AverageDuration(String aggregatePropertyKey, TimeDimension dimension) {
    this(aggregatePropertyKey, dimension, AbstractDurationAggregateFunction.DEFAULT_UNIT);
  }

  /**
   * Creates a new instance of this aggregate function. The the property key of the new property, where the
   * aggregated value is stored, will be defined as "avgDuration_" + {dimension} + "_" + {unit}. Use
   * constructor {@link AverageDuration#AverageDuration(String, TimeDimension, TemporalUnit)} to specify a
   * user-defined key.
   *
   * @param dimension the time dimension to consider
   * @param unit the temporal unit into which the result is converted. The supported units are specified in
   *             {@link AbstractDurationAggregateFunction#SUPPORTED_UNITS}.
   */
  public AverageDuration(TimeDimension dimension, TemporalUnit unit) {
    this("avgDuration_" + dimension + "_" + unit, dimension, unit);
  }

  /**
   * Get the duration of a time dimension as the aggregate value from a temporal element.
   * The duration will be returned in a format used by the {@link Average} aggregation.
   * The increment will be ignored, if the start of the end time of the time dimension is set
   * to a default value.
   *
   * @param element The temporal element.
   * @return The duration of the time dimension, in the internal representation used by {@link Average}.
   */
  @Override
  public PropertyValue getIncrement(TemporalElement element) {
    PropertyValue duration = getDuration(element);
    if (duration.getLong() == -1L) {
      return Average.IGNORED_VALUE;
    }
    return PropertyValue.create(Arrays.asList(duration, ONE));
  }

  /**
   * After calling {@link Average#postAggregate(PropertyValue)}, this function transforms the resulting
   * aggregate value to the specified temporal unit.
   *
   * @param result the result of the AverageDuration aggregation as {@link Double} in milliseconds, or null if
   *              nothing was aggregated.
   * @return By default (no time unit or the default unit [millis] is specified), the result of the
   *         AverageDuration aggregation as {@link Double} in milliseconds is returned. If a different
   *         temporal unit is given in {@link AbstractDurationAggregateFunction#timeUnit}, a {@link Double} of
   *         the desired unit is returned.
   * @throws UnsupportedTypeException if the type of the given property value is different from {@link Double}
   *                                  or null.
   * @see Average#postAggregate(PropertyValue)
   */
  @Override
  public PropertyValue postAggregate(PropertyValue result) throws UnsupportedTypeException {
    // First call the postAggregate of the interface to calculate the average aggregate
    result = Average.super.postAggregate(result);
    // Check if the aggregate value is of type null or double.
    if (!result.isNull() && !result.isDouble()) {
      throw new UnsupportedTypeException("The result type of the average duration aggregation must be " +
        "one of [" + Type.NULL + ", " + Type.DOUBLE + "], but is [" + result.getType() + "].");
    }
    if (timeUnit != AbstractDurationAggregateFunction.DEFAULT_UNIT && result.isDouble()) {
      // It is not a default unit, so we have to map the result value to the desired unit.
      result.setDouble(result.getDouble() / timeUnit.getDuration().toMillis());
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getSimpleName(), dimension);
  }
}
