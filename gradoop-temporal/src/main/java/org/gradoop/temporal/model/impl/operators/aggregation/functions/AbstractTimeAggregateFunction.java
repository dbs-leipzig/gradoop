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
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * An abstract super class for aggregation functions that aggregate a time of a temporal element.
 * Times can be a {@link TimeDimension.Field} of a {@link TimeDimension}.
 */
public abstract class AbstractTimeAggregateFunction extends BaseAggregateFunction
  implements TemporalAggregateFunction {

  /**
   * Selects which time-dimension is considered by this aggregate function.
   */
  private final TimeDimension timeDimension;

  /**
   * Selects the field of the temporal element to consider.
   */
  private final TimeDimension.Field field;

  /**
   * The property value that is considered as the default 'from' value of this aggregate function.
   * It is ignored during aggregation of valid times.
   */
  private final PropertyValue defaultFromValue = PropertyValue.create(TemporalElement.DEFAULT_TIME_FROM);

  /**
   * The property value that is considered as the default 'to' value of this aggregate function.
   * It is ignored during aggregation of valid times.
   */
  private final PropertyValue defaultToValue = PropertyValue.create(TemporalElement.DEFAULT_TIME_TO);

  /**
   * Sets attributes used to initialize this aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param timeDimension        The time dimension type to consider.
   * @param field                The field of the time-dimension to consider.
   */
  public AbstractTimeAggregateFunction(String aggregatePropertyKey, TimeDimension timeDimension,
    TimeDimension.Field field) {
    super(aggregatePropertyKey);
    this.timeDimension = Objects.requireNonNull(timeDimension);
    this.field = Objects.requireNonNull(field);
  }

  /**
   * Get a time stamp as the aggregate value from a temporal element.
   * The value will be the value of a {@link TimeDimension.Field} of a {@link TimeDimension}.
   *
   * @param element The temporal element.
   * @return The value, as a long-type property value.
   */
  @Override
  public PropertyValue getIncrement(TemporalElement element) {
    final Tuple2<Long, Long> timeInterval;
    switch (timeDimension) {
    case TRANSACTION_TIME:
      timeInterval = element.getTransactionTime();
      break;
    case VALID_TIME:
      timeInterval = element.getValidTime();
      break;
    default:
      throw new IllegalArgumentException("Unknown dimension [" + timeDimension + "].");
    }
    switch (field) {
    case FROM:
      return PropertyValue.create(timeInterval.f0);
    case TO:
      return PropertyValue.create(timeInterval.f1);
    default:
      throw new IllegalArgumentException("Field [" + field + "] is not supported for time intervals.");
    }
  }

  /**
   * Base aggregate function for min and max aggregations. Handles default behaviour and the
   * logic of the aggregation.
   *
   * @param aggregate The aggregate value.
   * @param increment The increment value.
   * @param comparison The function to apply the aggregation of the aggregate and increment value
   * @return the aggregated value
   */
  PropertyValue applyAggregateWithDefaults(PropertyValue aggregate, PropertyValue increment,
    BiFunction<PropertyValue, PropertyValue, PropertyValue> comparison) {
    if (aggregate.isNull() || isDefaultTime(aggregate)) {
      return isDefaultTime(increment) ? PropertyValue.NULL_VALUE : increment;
    } else if (increment.isNull() || isDefaultTime(increment)) {
      return aggregate;
    } else {
      return comparison.apply(aggregate, increment);
    }
  }

  /**
   * Checks if the given property value is a temporal default value (see {@link TemporalElement}).
   * If the temporal attribute is a {@link TimeDimension#TRANSACTION_TIME}, this function
   * will return {@code false} since the transaction time is system maintained and there are no
   * defaults to check.
   *
   * @param value the property value to check
   * @return true, if the time semantic is valid time and the value equals a default temporal value.
   */
  private boolean isDefaultTime(PropertyValue value) {
    return timeDimension == TimeDimension.VALID_TIME &&
      (value.equals(defaultFromValue) || value.equals(defaultToValue));
  }

  @Override
  public String toString() {
    return String.format("%s(%s.%s)", getClass().getSimpleName(), timeDimension, field);
  }
}
