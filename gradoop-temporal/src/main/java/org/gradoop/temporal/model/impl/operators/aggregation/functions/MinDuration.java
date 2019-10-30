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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.Min;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Calculate the minimum duration of a time dimension of one given {@link TimeDimension} of temporal elements.
 *
 * Time intervals with either the start or end time set to the respective default value are evaluated as
 * Long Max.
 */
public class MinDuration extends BaseAggregateFunction implements Min, TemporalAggregateFunction {

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
    this.dimension = dimension;
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
    Tuple2<Long, Long> timeInterval;
    switch (dimension) {
    case TRANSACTION_TIME:
      timeInterval = element.getTransactionTime();
      break;
    case VALID_TIME:
      timeInterval = element.getValidTime();
      break;
    default:
      throw new IllegalArgumentException("Temporal attribute " + dimension + " is not supported.");
    }
    if (timeInterval.f0 == null || timeInterval.f1 == null ||
      timeInterval.f0.equals(TemporalElement.DEFAULT_TIME_FROM) ||
      timeInterval.f0.equals(TemporalElement.DEFAULT_TIME_TO) ||
      timeInterval.f1.equals(TemporalElement.DEFAULT_TIME_FROM) ||
      timeInterval.f1.equals(TemporalElement.DEFAULT_TIME_TO)) {
      return PropertyValue.create(TemporalElement.DEFAULT_TIME_TO);

    } else {
      return PropertyValue.create(timeInterval.f1 - timeInterval.f0);
    }
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
