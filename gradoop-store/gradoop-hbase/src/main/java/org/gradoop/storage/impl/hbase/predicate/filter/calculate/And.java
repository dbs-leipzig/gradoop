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
package org.gradoop.storage.impl.hbase.predicate.filter.calculate;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Conjunctive predicate filter
 *
 * @param <T> element type
 */
public final class And<T extends EPGMElement> implements HBaseElementFilter<T> {

  /**
   * Predicate list
   */
  private final List<HBaseElementFilter<T>> predicates = new ArrayList<>();

  /**
   * Creates a new conjunctive filter chain
   *
   * @param predicates the predicates to combine with a logical and
   */
  private And(List<HBaseElementFilter<T>> predicates) {
    if (predicates.size() < 2) {
      throw new IllegalArgumentException(String.format("predicates len(=%d) < 2",
        predicates.size()));
    }
    this.predicates.addAll(predicates);
  }

  /**
   * Create a conjunctive formula
   *
   * @param predicates filter predicate
   * @param <T> input type
   * @return Conjunctive filter instance
   */
  @SafeVarargs
  public static <T extends EPGMElement> And<T> create(HBaseElementFilter<T>... predicates) {
    List<HBaseElementFilter<T>> formula = new ArrayList<>();
    Collections.addAll(formula, predicates);
    return new And<>(formula);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public Filter toHBaseFilter(boolean negate) {
    // If filter is negated, logical AND will be transformed to logical OR
    FilterList.Operator listOperator = negate ? FilterList.Operator.MUST_PASS_ONE :
      FilterList.Operator.MUST_PASS_ALL;
    FilterList filterList = new FilterList(listOperator);
    // Add each filter to filter list
    predicates.stream()
      .map(predicate -> predicate.toHBaseFilter(negate))
      .forEach(filterList::addFilter);

    return filterList;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" AND ");
    for (HBaseElementFilter<T> predicate : predicates) {
      joiner.add("(" + predicate.toString() + ")");
    }
    return joiner.toString();
  }
}
