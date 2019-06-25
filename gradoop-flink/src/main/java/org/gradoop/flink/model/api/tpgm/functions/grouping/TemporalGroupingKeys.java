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
package org.gradoop.flink.model.api.tpgm.functions.grouping;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.impl.operators.tpgm.grouping.keys.DurationKeyFunction;
import org.gradoop.flink.model.impl.operators.tpgm.grouping.keys.TimeIntervalKeyFunction;
import org.gradoop.flink.model.impl.operators.tpgm.grouping.keys.TimeStampKeyFunction;

import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;

/**
 * A factory class for creating instances of commonly used grouping key functions for grouping
 * on temporal attributes.
 */
public class TemporalGroupingKeys {
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
   */
  public static <T extends TemporalElement> GroupingKeyFunction<T, Long> duration(
    TemporalAttribute interval, TemporalUnit timeUnit) {
    return new DurationKeyFunction<>(interval, timeUnit);
  }

  /**
   * Group by a time interval. This will use a time interval as is for grouping.
   *
   * @param interval The time interval to group by.
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting a time interval.
   */
  public static <T extends TemporalElement> GroupingKeyFunction<T, Tuple2<Long, Long>> timeInterval(
    TemporalAttribute interval) {
    return new TimeIntervalKeyFunction<>(interval);
  }

  /**
   * Group by a time stamp. The time stamp will be in milliseconds.
   *
   * @param interval      The time interval to consider.
   * @param intervalField The field of that time interval to consider.
   * @param <T> The type of the elements to group.
   * @return The grouping key function extracting a time stamp.
   */
  public static <T extends TemporalElement> GroupingKeyFunction<T, Long> timeStamp(
    TemporalAttribute interval, TemporalAttribute.Field intervalField) {
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
   */
  public static <T extends TemporalElement> GroupingKeyFunction<T, Long> timeStamp(
    TemporalAttribute interval, TemporalAttribute.Field intervalField,
    TemporalField fieldOfTimeStamp) {
    return new TimeStampKeyFunction<>(interval, intervalField, fieldOfTimeStamp);
  }
}
