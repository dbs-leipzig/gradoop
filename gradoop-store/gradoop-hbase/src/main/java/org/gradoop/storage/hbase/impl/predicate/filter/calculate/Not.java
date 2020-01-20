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
package org.gradoop.storage.hbase.impl.predicate.filter.calculate;

import org.apache.hadoop.hbase.filter.Filter;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;

/**
 * Negated filter
 *
 * @param <T> input type
 */
public final class Not<T extends Element> implements HBaseElementFilter<T> {

  /**
   * The predicate to negate
   */
  private final HBaseElementFilter<T> predicate;

  /**
   * Create a new negated predicate
   *
   * @param predicate predicate
   */
  private Not(HBaseElementFilter<T> predicate) {
    this.predicate = predicate;
  }

  /**
   * Create a negative formula
   *
   * @param predicate predicate to negate
   * @param <T> input type
   * @return negated filter instance
   */
  public static <T extends Element> Not<T> of(HBaseElementFilter<T> predicate) {
    return new Not<>(predicate);
  }

  @Nonnull
  @Override
  public Filter toHBaseFilter(boolean negate) {
    // Toggle negation to prevent double negation while fetching filter instance
    return predicate.toHBaseFilter(!negate);
  }

  @Override
  public String toString() {
    return "NOT " + predicate;
  }
}
