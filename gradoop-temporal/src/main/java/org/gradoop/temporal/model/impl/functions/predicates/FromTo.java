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
package org.gradoop.temporal.model.impl.functions.predicates;

import org.gradoop.temporal.model.api.functions.TemporalPredicate;

import java.time.LocalDateTime;

import static org.gradoop.temporal.util.TimeFormatConversion.toEpochMilli;

/**
 * Implementation of the <b>FromTo</b> temporal predicate.
 * Given a time-interval, this predicate will match all intervals that were valid during that interval.
 * <p>
 * Predicate: {@code elementFrom < queryTo && elementTo > queryFrom}
 */
public class FromTo implements TemporalPredicate {

  /**
   * The start of the query time-interval in Milliseconds since Unix Epoch.
   */
  private final long queryFrom;

  /**
   * The end of the query time-interval in Milliseconds since Unix Epoch.
   */
  private final long queryTo;

  /**
   * Creates a <b>FromTo</b> instance with the given time stamps.
   *
   * @param from The start of the query time-interval in Milliseconds since Unix Epoch.
   * @param to   The end of the query time-interval in Milliseconds since Unix Epoch.
   */
  public FromTo(long from, long to) {
    queryFrom = from;
    queryTo = to;
  }

  /**
   * Creates a <b>FromTo</b> instance with the given time-interval.
   * The provided arguments will be converted to milliseconds since Unix Epoch for UTC time zone.
   *
   * @param from The beginning of the query time-interval.
   * @param to The end of the query time-interval.
   */
  public FromTo(LocalDateTime from, LocalDateTime to) {
    queryFrom = toEpochMilli(from);
    queryTo = toEpochMilli(to);
  }

  @Override
  public boolean test(long from, long to) {
    return from < queryTo && to > queryFrom;
  }

  @Override
  public String toString() {
    return "FROM " + queryFrom + " TO " + queryTo;
  }
}
