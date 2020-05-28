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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;

/**
 * Abstract base class for functions aggregating the duration of temporal elements for a certain {@link TimeDimension}.
 */
public abstract class AbstractDurationAggregateFunction extends BaseAggregateFunction {

  /**
   * Selects which time dimension is considered by this aggregate function.
   */
  protected final TimeDimension dimension;

  /**
   * Creates a new instance of this base aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   * @param dimension the given TimeDimension
   */
  AbstractDurationAggregateFunction(String aggregatePropertyKey, TimeDimension dimension) {
    super(aggregatePropertyKey);
    this.dimension = Objects.requireNonNull(dimension);
  }

  /**
   * Returns the duration of an element based on the given {@link TimeDimension}.
   * If start or end of the interval are `null` or set to a default value, {@code -1} is returned.
   *
   * @param element the given TemporalElement
   * @return a {@link PropertyValue} object holding the duration as long value [ms] or {@value -1} in case of a null or default value
   */
  PropertyValue getDuration(TemporalElement element) {
    Tuple2<Long, Long> timeInterval;
    switch (dimension) {
    case TRANSACTION_TIME:
      timeInterval = element.getTransactionTime();
      break;
    case VALID_TIME:
      timeInterval = element.getValidTime();
      break;
    default:
      throw new IllegalArgumentException("Unknown dimension [" + dimension + "].");
    }
    if (timeInterval.f0 == null || timeInterval.f1 == null ||
      timeInterval.f0.equals(TemporalElement.DEFAULT_TIME_FROM) ||
      timeInterval.f0.equals(TemporalElement.DEFAULT_TIME_TO) ||
      timeInterval.f1.equals(TemporalElement.DEFAULT_TIME_FROM) ||
      timeInterval.f1.equals(TemporalElement.DEFAULT_TIME_TO)) {
      return PropertyValue.create(-1L);
    } else {
      return PropertyValue.create(timeInterval.f1 - timeInterval.f0);
    }
  }
}


