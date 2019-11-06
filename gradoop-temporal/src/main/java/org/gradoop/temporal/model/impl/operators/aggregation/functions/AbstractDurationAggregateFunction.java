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
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Abstract class for the DurationAggregate Functions
 *
 */
public abstract class AbstractDurationAggregateFunction extends BaseAggregateFunction {


  /**
   * Creates a new instance of a base aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public AbstractDurationAggregateFunction(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }

  /**
   * Returns the duration of an element depending on the given TimeDimension
   * If at least one part of the interval is either null or a Default Time, the
   * method returns -1
   *
   * @param element the given TemporalElement
   * @param dimension the given TimeDimension
   * @return a correct duration or -1
   */
  public static PropertyValue getDuration(TemporalElement element, TimeDimension dimension) {
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
      return PropertyValue.create(-1L);
    } else {
      return PropertyValue.create(timeInterval.f1 - timeInterval.f0);
    }
  }
}


