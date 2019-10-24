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
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Arrays;
import java.util.Objects;

/**
 * Calculate the average duration of a time dimension of one given {@link TimeDimension} of temporal elements.
 * Time intervals with either the start or end time set to the respective default value will be ignored.
 */
public class AverageDuration extends BaseAggregateFunction implements Average, TemporalAggregateFunction {

  /**
   * A property value containing the number {@code 1}, as a {@link Long}.
   */
  private static final PropertyValue ONE = PropertyValue.create(1L);

  /**
   * Selects which time dimension is considered by this aggregate function.
   */
  private final TimeDimension dimension;

  /**
   * Create an instance of the {@link AverageDuration} aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param dimension            The time dimension to consider.
   */
  public AverageDuration(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey);
    this.dimension = Objects.requireNonNull(dimension);
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
      return Average.IGNORED_VALUE;
    } else {
      return PropertyValue
        .create(Arrays.asList(PropertyValue.create(timeInterval.f1 - timeInterval.f0), ONE));
    }
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getSimpleName(), dimension);
  }
}
