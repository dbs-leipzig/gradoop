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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Conjunctive predicate filter
 *
 * @param <T> element type
 */
public final class And<T extends EPGMElement> implements AccumuloElementFilter<T> {

  /**
   * Predicate list
   */
  private final List<AccumuloElementFilter<T>> predicates = new ArrayList<>();

  /**
   * Create a new conjunctive principles
   *
   * @param predicates predicates
   */
  private And(List<AccumuloElementFilter<T>> predicates) {
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
  public static <T extends EPGMElement> And<T> create(AccumuloElementFilter<T>... predicates) {
    List<AccumuloElementFilter<T>> formula = new ArrayList<>();
    Collections.addAll(formula, predicates);
    return new And<>(formula);
  }

  @Override
  public boolean test(T t) {
    for (AccumuloElementFilter<T> predicate : predicates) {
      if (!predicate.test(t)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" AND ");
    for (AccumuloElementFilter<T> predicate : predicates) {
      joiner.add("(" + predicate.toString() + ")");
    }
    return joiner.toString();
  }

}
