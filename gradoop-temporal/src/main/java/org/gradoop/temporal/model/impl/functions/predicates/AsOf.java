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
 * Implementation of the <b>AsOf</b> predicate.
 * Given a certain time-stamp, this predicate will match all time-stamps before or at that time
 * and all time-intervals containing that time.
 * <p>
 * Predicate: elementFrom <= queryTimestamp && elementTo > queryTimestamp
 */
public class AsOf implements TemporalPredicate {

  /**
   * The queryTimestamp to be matched.
   */
  private final long queryTimestamp;

  /**
   * Creates a AsOf instance with the given time-stamp.
   *
   * @param timestamp The timestamp to match in Milliseconds since Unix Epoch.
   */
  public AsOf(long timestamp) {
    this.queryTimestamp = timestamp;
  }

  @Override
  public boolean test(long from, long to) {
    return from <= queryTimestamp && to > queryTimestamp;
  }

  @Override
  public String toString() {
    return "AS OF " + queryTimestamp;
  }
}
