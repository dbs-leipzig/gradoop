/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.Min;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;

/**
 * Calculate the minimum duration of a time dimension of one given {@link TimeDimension} of temporal elements.
 *
 * Time intervals with either the start or end time set to the respective default value are evaluated as
 * Long Max.
 */
public class MinDuration extends AbstractDurationAggregateFunction implements Min, TemporalAggregateFunction {

  /**
   * Selects which time dimension is considered by this aggregate function.
   */
  private final TimeDimension dimension;

  /**
   * Creates a new instance of a base aggregate function.
   *
   * @param aggregatePropertyKey the given aggregate property key
   * @param dimension the given TimeDimension
   */
  public MinDuration(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey);
    this.dimension = Objects.requireNonNull(dimension);
  }

  /**
   * Calculates the duration of a given element depending on the TimeDimension of the MaxDuration function.
   *  Returns Long_Max if either the start or end time of the duration are equal to
   *  DEFAULT_TIME_FROM / DEFAULT_TIME_TO or null
   *
   * @param element the temporal element
   * @return the duration of the time interval
   */
  @Override
  public PropertyValue getIncrement(TemporalElement element) {
    PropertyValue duration = super.getDuration(element, dimension);
    if (duration.getLong() == -1L) {
      return PropertyValue.create(TemporalElement.DEFAULT_TIME_TO);
    }
    return duration;
  }

  /**
   * The aggregate function returns the shortest of both durations
   *
   * @param aggregate previously aggregated value
   * @param increment value that is added to the aggregate
   *
   * @return the minimum of aggregate and increment
   */
  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Numeric.min(aggregate, increment);
  }
}
