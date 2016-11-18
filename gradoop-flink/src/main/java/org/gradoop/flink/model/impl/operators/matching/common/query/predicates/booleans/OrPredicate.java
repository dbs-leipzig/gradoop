/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
