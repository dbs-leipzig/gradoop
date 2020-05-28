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
package org.gradoop.temporal.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.temporal.model.api.TimeDimension;
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
 * extracted from the element. When no {@link TemporalField} is given, the property will just be called
 * {@code time_INTERVAL_FIELD}.<p>
 * When the extracted {@link TimeDimension.Field field} of the {@link TimeDimension} is set to a default
 * value and a {@link TemporalField} was set, a default value ({@code -1}) will be returned instead.
 * In this case no property will be set.
 *
 * @param <T> The type of the elements to group.
 */
public class TimeStampKeyFunction<T extends TemporalElement> implements KeyFunctionWithDefaultValue<T, Long> {

  /**
   * The time dimension of the temporal element to extract.
   */
  private final TimeDimension timeDimension;

  /**
   * The field of that dimension to extract.
   */
  private final TimeDimension.Field timeDimensionField;

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
   * @param timeDimension      The time dimension of the temporal element to consider.
   * @param timeDimensionField The field of that time dimension to consider.
   * @param fieldOfTimeStamp   The time field of that field to calculate. May be {@code null},
   *                           in that case nothing will be calculated, the time stamp will be
   *                           returned as is.
   */
  public TimeStampKeyFunction(TimeDimension timeDimension,
    TimeDimension.Field timeDimensionField, TemporalField fieldOfTimeStamp) {
    this.timeDimension = Objects.requireNonNull(timeDimension);
    this.timeDimensionField = Objects.requireNonNull(timeDimensionField);
    this.fieldOfTimeStamp = fieldOfTimeStamp;
    this.targetPropertyKey = "time_" + timeDimension + "_" + timeDimensionField +
      (fieldOfTimeStamp != null ? "_" + fieldOfTimeStamp : "");
  }

  @Override
  public Long getKey(T element) {
    final Tuple2<Long, Long> interval;
    switch (timeDimension) {
    case TRANSACTION_TIME:
      interval = element.getTransactionTime();
      break;
    case VALID_TIME:
      interval = element.getValidTime();
      break;
    default:
      throw new UnsupportedOperationException(
        "Time interval not supported by this element: " + timeDimension);
    }
    final Long fieldValue;
    switch (timeDimensionField) {
    case FROM:
      fieldValue = interval.f0;
      if (fieldValue.equals(TemporalElement.DEFAULT_TIME_FROM) && (fieldOfTimeStamp != null)) {
        return getDefaultKey();
      }
      break;
    case TO:
      fieldValue = interval.f1;
      if (fieldValue.equals(TemporalElement.DEFAULT_TIME_TO) && (fieldOfTimeStamp != null)) {
        return getDefaultKey();
      }
      break;
    default:
      throw new UnsupportedOperationException("Field is not supported: " + timeDimensionField);
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
    // Do not set the key if field extraction is enabled and the key is -1
    if (fieldOfTimeStamp == null || !getDefaultKey().equals(key)) {
      element.setProperty(targetPropertyKey, PropertyValue.create(key));
    }
  }

  @Override
  public TypeInformation<Long> getType() {
    return BasicTypeInfo.LONG_TYPE_INFO;
  }

  @Override
  public Long getDefaultKey() {
    if (fieldOfTimeStamp == null) {
      switch (timeDimensionField) {
      case FROM:
        return TemporalElement.DEFAULT_TIME_FROM;
      case TO:
        return TemporalElement.DEFAULT_TIME_TO;
      default:
        throw new UnsupportedOperationException("Field not supported: " + timeDimensionField);
      }
    }
    return -1L;
  }
}
