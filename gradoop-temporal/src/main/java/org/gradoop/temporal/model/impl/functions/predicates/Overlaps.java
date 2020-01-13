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
package org.gradoop.temporal.model.impl.functions.predicates;

import org.gradoop.temporal.model.api.functions.TemporalPredicate;

import java.time.LocalDateTime;

import static org.gradoop.temporal.util.TimeFormatConversion.toEpochMilli;

/**
 * Implementation of the <b>Overlaps</b> temporal predicate.
 * Given a certain time interval, this predicates matches all intervals that overlap with that interval.
 * <p>
 * Predicate: {@code max(from, queryFrom) < min(to, queryTo)}
 */
public class Overlaps implements TemporalPredicate {

  /**
   * Beginning of query time-interval to be matched.
   */
  private final long queryFrom;

  /**
   * End of query time-interval to be matched.
   */
  private final long queryTo;

  /**
   * Creates a <b>Overlaps</b> instance with the given time-interval.
   * The provided arguments will be converted to milliseconds since Unix Epoch for UTC time zone.
   *
   * @param from The beginning of the query time-interval
   * @param to   The end of the query time-interval.
   */
  public Overlaps(LocalDateTime from, LocalDateTime to) {
    this.queryFrom = toEpochMilli(from);
    this.queryTo = toEpochMilli(to);
  }

  /**
   * Creates a <b>Overlaps</b> instance with the given time-interval.
   *
   * @param from The beginning of the interval to match in milliseconds since Unix Epoch.
   * @param to   The end of the interval to match in milliseconds since Unix Epoch.
   */
  public Overlaps(long from, long to) {
    this.queryFrom = from;
    this.queryTo = to;
  }

  @Override
  public boolean test(long from, long to) {
    return Math.max(from, queryFrom) < Math.min(to, queryTo);
  }

  @Override
  public String toString() {
    return String.format("OVERLAPS (%d, %d)", queryFrom, queryTo);
  }
}
