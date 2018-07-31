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
import org.s1ck.gdl.model.predicates.booleans.And;

/**
 * Wraps an {@link org.s1ck.gdl.model.predicates.booleans.And} predicate
 */
public class AndPredicate extends QueryPredicate {
  /**
   * Holds the wrapped predicate
   */
  private final And and;

  /**
   * Returns a new AndPredicate
   * @param and the predicate
   */
  public AndPredicate(And and) {
    this.and = and;
  }

  /**
   * Converts the predicate into conjunctive normal form
   * @return predicate in CNF
   */
  public CNF asCNF() {
    return getLhs().asCNF()
      .and(getRhs().asCNF());
  }

  /**
   * Retruns the wrapped left hand side predicate
   * @return the left hand side
   */
  public QueryPredicate getLhs() {
    return QueryPredicate.createFrom(and.getArguments()[0]);
  }

  /**
   * Retruns the wrapped right hand side predicate
   * @return the right hand side
   */
  public QueryPredicate getRhs() {
    return QueryPredicate.createFrom(and.getArguments()[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AndPredicate that = (AndPredicate) o;

    return and != null ? and.equals(that.and) : that.and == null;
  }

  @Override
  public int hashCode() {
    return and != null ? and.hashCode() : 0;
  }
}
