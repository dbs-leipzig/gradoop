/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.Min;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Calculates the minimum duration of a {@link TimeDimension} of temporal elements.
 *
 * Time intervals with either the start or end time set to the respective default value are evaluated as
 * {@link Long#MAX_VALUE}.
 */
public class MinDuration extends AbstractDurationAggregateFunction implements Min, TemporalAggregateFunction {

  /**
   * Creates a new instance of this base aggregate function.
   *
   * @param aggregatePropertyKey the given aggregate property key
   * @param dimension the time dimension to consider
   */
  public MinDuration(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey, dimension);
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
   * Method to check whether all aggregated durations had been default values.
   *
   * @param result the result of the MinDuration Aggregation
   * @return null, if the minimum duration is {@link TemporalElement#DEFAULT_TIME_TO}
   */
  @Override
  public PropertyValue postAggregate(PropertyValue result) {
    if (result.getLong() == TemporalElement.DEFAULT_TIME_TO) {
      return PropertyValue.NULL_VALUE;
    }
    return result;
  }
}
