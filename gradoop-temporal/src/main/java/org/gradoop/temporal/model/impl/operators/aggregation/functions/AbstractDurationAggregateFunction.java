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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Abstract base class for functions aggregating the duration of temporal elements for a certain {@link TimeDimension}.
 */
public abstract class AbstractDurationAggregateFunction extends BaseAggregateFunction {

  /**
   * The default temporal unit.
   */
  public static final TemporalUnit DEFAULT_UNIT = ChronoUnit.MILLIS;

  /**
   * A list of supported temporal units.
   */
  static final List<TemporalUnit> SUPPORTED_UNITS = Arrays.asList(ChronoUnit.MILLIS, ChronoUnit.SECONDS,
    ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS);

  /**
   * Selects which time dimension is considered by this aggregate function.
   */
  protected final TimeDimension dimension;

  /**
   * The unit to parse the duration after the aggregation.
   */
  protected final TemporalUnit timeUnit;

  /**
   * Creates a new instance of this base aggregate function.
   *
   * @param aggregatePropertyKey the property key of the new property where the aggregated value is stored
   * @param dimension the given TimeDimension
   * @param unit the temporal unit into which the result is converted. The supported units are specified in
   *             {@link AbstractDurationAggregateFunction#SUPPORTED_UNITS}.
   */
  AbstractDurationAggregateFunction(String aggregatePropertyKey, TimeDimension dimension, TemporalUnit unit) {
    super(aggregatePropertyKey);
    this.dimension = Objects.requireNonNull(dimension);
    Objects.requireNonNull(unit);
    if (!SUPPORTED_UNITS.contains(unit)) {
      throw new IllegalArgumentException("The given unit [" + unit +
        "] is not supported. Supported temporal units are [" +
        SUPPORTED_UNITS.stream().map(TemporalUnit::toString).collect(Collectors.joining(",")) + "]");
    }
    this.timeUnit = unit;
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


