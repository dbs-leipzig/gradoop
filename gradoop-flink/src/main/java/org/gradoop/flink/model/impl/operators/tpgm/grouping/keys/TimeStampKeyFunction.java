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
package org.gradoop.flink.model.impl.operators.tpgm.grouping.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.api.tpgm.functions.grouping.GroupingKeyFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.TemporalField;
import java.util.Objects;

import static java.time.ZoneOffset.UTC;

/**
 * A key function extracting a time stamp of a {@link TemporalElement}.
 *
 * @param <T> The type of the elements to group.
 */
public class TimeStampKeyFunction<T extends TemporalElement>
  implements GroupingKeyFunction<T, Long> {

  /**
   * The time interval of the temporal element to extract.
   */
  private final TemporalAttribute timeInterval;

  /**
   * The field of that interval to extract.
   */
  private final TemporalAttribute.Field timeIntervalField;

  /**
   * The time field to calculate.
   */
  private final TemporalField fieldOfTimeStamp;

  /**
   * Create a new instance of this grouping key function.
   *
   * @param timeInterval      The time interval of the temporal element to consider.
   * @param timeIntervalField The field of that time interval to consider.
   * @param fieldOfTimeStamp  The time field of that field to calculate. May be {@code null},
   *                          in that case nothing will be calculated, the time stamp will be
   *                          returned as is.
   */
  public TimeStampKeyFunction(TemporalAttribute timeInterval,
    TemporalAttribute.Field timeIntervalField, TemporalField fieldOfTimeStamp) {
    this.timeInterval = Objects.requireNonNull(timeInterval);
    this.timeIntervalField = Objects.requireNonNull(timeIntervalField);
    this.fieldOfTimeStamp = fieldOfTimeStamp;
  }

  @Override
  public Long getKey(T element) {
    final Tuple2<Long, Long> interval;
    switch (timeInterval) {
    case TRANSACTION_TIME:
      interval = element.getTransactionTime();
      break;
    case VALID_TIME:
      interval = element.getValidTime();
      break;
    default:
      throw new UnsupportedOperationException(
        "Time interval not supported by this element: " + timeInterval);
    }
    final Long fieldValue;
    switch (timeIntervalField) {
    case FROM:
      fieldValue = interval.f0;
      break;
    case TO:
      fieldValue = interval.f1;
      break;
    default:
      throw new UnsupportedOperationException("Field is not supported: " + timeIntervalField);
    }
    if (fieldOfTimeStamp == null) {
      return fieldValue;
    }
    final LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(fieldValue), UTC);
    return fieldOfTimeStamp.getFrom(date);
  }

  @Override
  public TypeInformation<Long> getType() {
    return BasicTypeInfo.LONG_TYPE_INFO;
  }
}
