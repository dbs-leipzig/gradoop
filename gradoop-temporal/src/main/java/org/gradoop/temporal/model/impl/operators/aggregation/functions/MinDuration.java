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
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.Min;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.temporal.TemporalUnit;

/**
 * Calculates the minimum duration of a {@link TimeDimension} of temporal elements.
 *
 * Time intervals with either the start or end time set to the respective default value are evaluated as zero.
 */
public class MinDuration extends AbstractDurationAggregateFunction implements Min, TemporalAggregateFunction {

  /**
   * Creates a new instance of this aggregate function.
   *
   * @param aggregatePropertyKey the property key of the new property where the aggregated value is stored
   * @param dimension the time dimension to consider
   * @param unit the temporal unit into which the result is converted. The supported units are specified in
   *             {@link AbstractDurationAggregateFunction#SUPPORTED_UNITS}.
   */
  public MinDuration(String aggregatePropertyKey, TimeDimension dimension, TemporalUnit unit) {
    super(aggregatePropertyKey, dimension, unit);
  }

  /**
   * Creates a new instance of this aggregate function.
   *
   * @param aggregatePropertyKey the property key of the new property where the aggregated value is stored
   * @param dimension the time dimension to consider
   */
  public MinDuration(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey, dimension, AbstractDurationAggregateFunction.DEFAULT_UNIT);
  }

  /**
   * Creates a new instance of this aggregate function. The the property key of the new property, where the
   * aggregated value is stored, will be defined as "minDuration_" + {dimension} + "_" + {unit}. Use
   * constructor {@link MinDuration#MinDuration(String, TimeDimension, TemporalUnit)} to specify a
   * user-defined key.
   *
   * @param dimension the time dimension to consider
   * @param unit the temporal unit into which the result is converted. The supported units are specified in
   *             {@link AbstractDurationAggregateFunction#SUPPORTED_UNITS}.
   */
  public MinDuration(TimeDimension dimension, TemporalUnit unit) {
    this("minDuration_" + dimension + "_" + unit, dimension, unit);
  }

  /**
   * Calculates the duration of a given element depending on the given {@link TimeDimension}.
   * Returns {@link Long#MAX_VALUE} if either the start or end time of the duration are default values.
   *
   * @param element the temporal element
   * @return the duration of the time interval
   */
  @Override
  public PropertyValue getIncrement(TemporalElement element) {
    PropertyValue duration = getDuration(element);
    if (duration.getLong() == -1L) {
      return PropertyValue.create(TemporalElement.DEFAULT_TIME_TO);
    }
    return duration;
  }

  /**
   * Method to check whether all aggregated durations had been default values or need to be transformed to
   * another unit.
   *
   * @param result the result of the MinDuration aggregation as {@link Long} in milliseconds
   * @return By default (no time unit or the default unit [millis] is specified), the result of the
   *         MinDuration Aggregation as {@link Long} in milliseconds is returned. If a different time unit is
   *         given in {@link AbstractDurationAggregateFunction#timeUnit}, a {@link Double} of
   *         the desired unit is returned. If the minimum duration is {@link TemporalElement#DEFAULT_TIME_TO},
   *         the value {@link PropertyValue#NULL_VALUE}is returned.
   * @throws UnsupportedTypeException if the type of the given property value is different from {@link Long}.
   */
  @Override
  public PropertyValue postAggregate(PropertyValue result) throws UnsupportedTypeException {
    // First check if the aggregated result is of type long.
    if (!result.isLong()) {
      throw new UnsupportedTypeException("The result type of the min duration aggregation must be [" +
        Type.LONG + "], but is [" + result.getType() + "].");
    }
    // If the result is a default value, set it as null.
    if (result.getLong() == TemporalElement.DEFAULT_TIME_TO) {
      return PropertyValue.NULL_VALUE;
    }
    // If a time unit is specified, convert it.
    if (timeUnit != AbstractDurationAggregateFunction.DEFAULT_UNIT) {
      result.setDouble((double) result.getLong() / timeUnit.getDuration().toMillis());
    }
    return result;
  }
}
