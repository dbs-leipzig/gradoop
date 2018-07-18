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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.s1ck.gdl.model.predicates.booleans.Or;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.Or} predicate
 */
public class OrPredicate extends QueryPredicate {
  /**
   * Holds the wrapped or predicate
   */
  private final Or or;

  /**
   * Creates a new or wrapper
   * @param or the wrapped or predicate
   */
  public OrPredicate(Or or) {
    this.or = or;
  }

  /**
   * Converts the predicate into conjunctive normal form
   * @return predicate in cnf
   */
  public CNF asCNF() {
    return getLhs().asCNF().or(getRhs().asCNF());
  }

  /**
   * Returns the left hand side predicate
   * @return the left hand side predicate
   */
  public QueryPredicate getLhs() {
    return QueryPredicate.createFrom(or.getArguments()[0]);
  }

  /**
   * Returns the right hand side predicate
   * @return the right hand side predicate
   */
  public QueryPredicate getRhs() {
    return QueryPredicate.createFrom(or.getArguments()[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OrPredicate orPredicateWrapper = (OrPredicate) o;

    return or != null ? or.equals(orPredicateWrapper.or) : orPredicateWrapper.or == null;
  }

  @Override
  public int hashCode() {
    return or != null ? or.hashCode() : 0;
  }
}
