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
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Objects;

import static java.time.ZoneOffset.UTC;

/**
 * A key function extracting the duration of a temporal attribute.<p>
 * This key function extracts a time interval from an element and calculates the duration of that element
 * in a certain {@link TemporalUnit} (e.g. {@link ChronoUnit#SECONDS seconds}).
 * Calculations will assume the time zone to be {@link java.time.ZoneOffset#UTC UTC}.<p>
 * The final grouping key will be stored on the super element as a property with key
 * {@code duration_INTERVAL_UNIT} where {@code INTERVAL} is the {@link TimeDimension} extracted and
 * {@code UNIT} the {@link TemporalUnit} to be calculated.
 *
 * @param <T> The type of the temporal elements.
 */
public class DurationKeyFunction<T extends TemporalElement> implements KeyFunction<T, Long> {

  /**
   * A key function used to extract the interval from the element.
   */
  private final TimeIntervalKeyFunction<T> intervalExtractor;

  /**
   * The unit to get the duration as.
   */
  private final TemporalUnit timeUnit;

  /**
   * Should the duration be calculated from dates instead of {@link Instant}s? This will support
   * units larger than {@link ChronoUnit#DAYS}.
   */
  private final boolean calculateAsDate;

  /**
   * Create a new instance of this grouping key function.
   *
   * @param timeDimension The time dimension to get the duration of.
   * @param unit          The unit to get the duration as.
   */
  public DurationKeyFunction(TimeDimension timeDimension, TemporalUnit unit) {
    this.intervalExtractor = new TimeIntervalKeyFunction<>(timeDimension);
    this.timeUnit = Objects.requireNonNull(unit);
    if (unit instanceof ChronoUnit) {
      calculateAsDate = ((ChronoUnit) unit).compareTo(ChronoUnit.DAYS) >= 0;
    } else {
      calculateAsDate = false;
    }
  }

  @Override
  public Long getKey(T element) {
    final Tuple2<Long, Long> interval = intervalExtractor.getKey(element);
    final Instant start = Instant.ofEpochMilli(interval.f0);
    final Instant end = Instant.ofEpochMilli(interval.f1);
    if (calculateAsDate) {
      return LocalDateTime.ofInstant(start, UTC).until(LocalDateTime.ofInstant(end, UTC), timeUnit);
    } else {
      return start.until(end, timeUnit);
    }
  }

  @Override
  public TypeInformation<Long> getType() {
    return BasicTypeInfo.LONG_TYPE_INFO;
  }

  @Override
  public void addKeyToElement(T element, Object key) {
    if (!(key instanceof Long)) {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
    element.setProperty("duration_" + intervalExtractor + "_" + timeUnit, PropertyValue.create(key));
  }
}
