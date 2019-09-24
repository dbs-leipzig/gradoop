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
package org.gradoop.temporal.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.temporal.model.api.functions.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.Objects;

import static java.time.ZoneOffset.UTC;

/**
 * A key function extracting a time stamp of a {@link TemporalElement}.<p>
 * An optional {@link TemporalField} parameter can be given to this key function. In this case the time stamp
 * will be converted to a date (with a time zone set to {@link java.time.ZoneOffset#UTC UTC}) and the field
 * (e. g. {@link ChronoField#MONTH_OF_YEAR month}) will be extracted from that date.<p>
 * When no {@link TemporalField} is given, the time stamp will be used as is, in milliseconds since unix
 * epoch.<p>
 * The final grouping key will be stored on the super element as a property with key
 * {@code time_INTERVAL_FIELD_CALCULATEDFIELD} where {@code INTERVAL} is the {@link TimeDimension},
 * {@code FIELD} the {@link TimeDimension.Field} and {@code CALCULATEDFIELD} the {@link TemporalField}
 * extraced from the element. When no {@link TemporalField} is given, the property will just be called
 * {@code time_INTERVAL_FIELD}.
 *
 * @param <T> The type of the elements to group.
 */
public class TimeStampKeyFunction<T extends TemporalElement> implements KeyFunction<T, Long> {

  /**
   * The time interval of the temporal element to extract.
   */
  private final TimeDimension timeInterval;

  /**
   * The field of that interval to extract.
   */
  private final TimeDimension.Field timeIntervalField;

  /**
   * The time field to calculate.
   */
  private final TemporalField fieldOfTimeStamp;

  /**
   * The property key used to store the grouping key on the super-element.
   */
  private final String targetPropertyKey;

  /**
   * Create a new instance of this grouping key function.
   *
   * @param timeInterval      The time interval of the temporal element to consider.
   * @param timeIntervalField The field of that time interval to consider.
   * @param fieldOfTimeStamp  The time field of that field to calculate. May be {@code null},
   *                          in that case nothing will be calculated, the time stamp will be
   *                          returned as is.
   */
  public TimeStampKeyFunction(TimeDimension timeInterval,
    TimeDimension.Field timeIntervalField, TemporalField fieldOfTimeStamp) {
    this.timeInterval = Objects.requireNonNull(timeInterval);
    this.timeIntervalField = Objects.requireNonNull(timeIntervalField);
    this.fieldOfTimeStamp = fieldOfTimeStamp;
    this.targetPropertyKey = "time_" + timeInterval + "_" + timeIntervalField +
      (fieldOfTimeStamp != null ? "_" + fieldOfTimeStamp : "");
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
  public void addKeyToElement(T element, Object key) {
    if (!(key instanceof Long)) {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
    element.setProperty(targetPropertyKey, PropertyValue.create(key));
  }

  @Override
  public TypeInformation<Long> getType() {
    return BasicTypeInfo.LONG_TYPE_INFO;
  }
}
