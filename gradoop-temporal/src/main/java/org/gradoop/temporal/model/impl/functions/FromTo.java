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
package org.gradoop.temporal.model.impl.functions;

import org.gradoop.temporal.model.api.functions.TemporalPredicate;

/**
 * Implementation of the <b>FromTo</b> temporal predicate.
 * Given a certain time-interval, this predicate will match all intervals that were valid during
 * that interval.
 */
public class FromTo implements TemporalPredicate {

  /**
   * The start of the query time-interval.
   */
  private final long queryFrom;

  /**
   * The end of the query time-interval.
   */
  private final long queryTo;

  /**
   * Creates a FromTo instance with the given time stamps.
   *
   * @param from The start of the query time-interval.
   * @param to   The end of the query time-interval.
   */
  public FromTo(long from, long to) {
    queryFrom = from;
    queryTo = to;
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
