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
package org.gradoop.temporal.model.impl.operators.groupingng.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.functions.GroupingKeyFunction;
import org.gradoop.temporal.model.api.functions.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Objects;

import static java.time.ZoneOffset.UTC;

/**
 * A key function extracting the duration of a temporal attribute.
 */
public class DurationKeyFunction<T extends TemporalElement>
  implements GroupingKeyFunction<T, Long> {

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
   * @param timeInterval The time interval to get the duration of.
   * @param unit         The unit to get the duration as.
   */
  public DurationKeyFunction(TimeDimension timeInterval, TemporalUnit unit) {
    this.intervalExtractor = new TimeIntervalKeyFunction<>(timeInterval);
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
       return LocalDateTime.ofInstant(start, UTC)
         .until(LocalDateTime.ofInstant(end, UTC), timeUnit);
    } else {
      return start.until(end, timeUnit);
    }
  }

  @Override
  public TypeInformation<Long> getType() {
    return BasicTypeInfo.LONG_TYPE_INFO;
  }

  @Override
  public String getTargetPropertyKey() {
    return "duration_" + intervalExtractor + "_" + timeUnit;
  }
}
