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
package org.gradoop.temporal.model.api.functions;

import java.io.Serializable;

/**
 * A predicate that selects certain time-stamps or time-intervals.
 */
@FunctionalInterface
public interface TemporalPredicate extends Serializable {
  /**
   * Evaluates this predicate for a certain time-stamp or -interval.
   * If this predicate only operates on time-stamps, the second argument will be ignored.
   *
   * @param from The start of the time-interval. (Or the time-stamp.)
   * @param to   The end of the time-interval. (Ignored, if this predicate only operates on
   *             time-stamps.)
   * @return {@code true}, if the time-interval /-stamp matches this predicate.
   */
  boolean test(long from, long to);
}
