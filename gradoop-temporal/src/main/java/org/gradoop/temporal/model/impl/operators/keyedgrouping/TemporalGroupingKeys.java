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
package org.gradoop.temporal.model.impl.operators.keyedgrouping;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.keys.DurationKeyFunction;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.keys.TimeIntervalKeyFunction;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.keys.TimeStampKeyFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

/**
 * A factory class for creating instances of commonly used grouping key functions for grouping
 * on temporal attributes.
 */
public final class TemporalGroupingKeys {
  /**
   * No instances of this class are needed.
   */
  private TemporalGroupingKeys() {
  }

  /**
   * Group by duration of a time interval. The duration will be calculated in a certain unit.
   *
   * @param interval The time interval to get the duration of.
   * @param timeUnit The unit in which the duration is measured.
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting the duration.
   * @see DurationKeyFunction
   */
  public static <T extends TemporalElement> KeyFunction<T, Long> duration(
    TimeDimension interval, TemporalUnit timeUnit) {
    return new DurationKeyFunction<>(interval, timeUnit);
  }

  /**
   * Group by a time interval. This will use a time interval as is for grouping.
   *
   * @param interval The time interval to group by.
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting a time interval.
   * @see TimeIntervalKeyFunction
   */
  public static <T extends TemporalElement> KeyFunction<T, Tuple2<Long, Long>> timeInterval(
    TimeDimension interval) {
    return new TimeIntervalKeyFunction<>(interval);
  }

  /**
   * Group by a time stamp. The time stamp will be in milliseconds.
   *
   * @param interval      The time interval to consider.
   * @param intervalField The field of that time interval to consider.
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting a time stamp.
   * @see TimeStampKeyFunction
   */
  public static <T extends TemporalElement> KeyFunctionWithDefaultValue<T, Long> timeStamp(
    TimeDimension interval, TimeDimension.Field intervalField) {
    return timeStamp(interval, intervalField, null);
  }

  /**
   * Group by a field of a time stamp. The time stamp will be converted to a date with timezone
   * {@link java.time.ZoneOffset#UTC UTC} and the field will be extracted from that date.
   * If the field is {@code null}, nothing will be extracted and the time stamp will be
   * used as is.
   *
   * @param interval         The time interval to consider.
   * @param intervalField    The field of that interval to consider (i.e. the start- or end-time).
   * @param fieldOfTimeStamp The field of the date corresponding to that time stamp to consider.
   *                         (May be {@code null}, in that case the time stamp will be used as is.)
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting the time stamp or part of it.
   * @see TimeStampKeyFunction
   */
  public static <T extends TemporalElement> KeyFunctionWithDefaultValue<T, Long> timeStamp(
    TimeDimension interval, TimeDimension.Field intervalField,
    TemporalField fieldOfTimeStamp) {
    return new TimeStampKeyFunction<>(interval, intervalField, fieldOfTimeStamp);
  }
}
