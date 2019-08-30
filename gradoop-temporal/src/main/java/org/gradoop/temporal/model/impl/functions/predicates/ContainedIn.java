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

/**
 * Implementation of the <b>ContainedIn</b> temporal predicate.
 * Given a certain time interval, this predicate will match all intervals that are a subset of that interval.
 * <p>
 * Predicate: queryFrom <= elementFrom && elementTo <= queryTo
 */
public class ContainedIn implements TemporalPredicate {

  /**
   * The start of the query time-interval in Milliseconds since Unix Epoch.
   */
  private final long queryFrom;

  /**
   * The end of the query time-interval in Milliseconds since Unix Epoch.
   */
  private final long queryTo;

  /**
   * Creates a ContainedIn instance with the given time stamps.
   *
   * @param from The start of the query time-interval in Milliseconds since Unix Epoch.
   * @param to   The end of the query time-interval in Milliseconds since Unix Epoch.
   */
  public ContainedIn(long from, long to) {
    queryFrom = from;
    queryTo = to;
  }

  @Override
  public boolean test(long from, long to) {
    return queryFrom <= from && to <= queryTo;
  }

  @Override
  public String toString() {
    return String.format("CONTAINED IN (%d, %d)", queryFrom, queryTo);
  }
}
