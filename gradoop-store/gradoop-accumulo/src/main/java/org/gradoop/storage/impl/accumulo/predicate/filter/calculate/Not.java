/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.storage.impl.accumulo.predicate.filter.calculate;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;

/**
 * Negative logic filter
 *
 * @param <T> input type
 */
public final class Not<T extends EPGMElement> implements AccumuloElementFilter<T> {

  /**
   * The predicate to negate
   */
  private final AccumuloElementFilter<T> predicate;

  /**
   * Create a new negated predicate
   *
   * @param predicate predicate
   */
  private Not(AccumuloElementFilter<T> predicate) {
    this.predicate = predicate;
  }

  /**
   * Create a negative formula
   *
   * @param predicate negative predicate
   * @param <T> input type
   * @return negative filter instance
   */
  public static <T extends EPGMElement> Not<T> of(AccumuloElementFilter<T> predicate) {
    return new Not<>(predicate);
  }

  @Override
  public boolean test(T t) {
    return !predicate.test(t);
  }

  @Override
  public String toString() {
    return "NOT " + predicate;
  }
}
