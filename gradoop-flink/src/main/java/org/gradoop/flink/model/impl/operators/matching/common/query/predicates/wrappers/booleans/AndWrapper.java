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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.PredicateWrapper;
import org.s1ck.gdl.model.predicates.booleans.And;

/**
 * Wraps an {@link And} predicate
 */
public class AndWrapper extends PredicateWrapper {
  /**
   * Holds the wrapped predicate
   */
  private final And and;

  /**
   * Returns a new AndWrapper
   * @param and the predicate
   */
  public AndWrapper(And and) {
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
  public PredicateWrapper getLhs() {
    return PredicateWrapper.wrap(and.getArguments()[0]);
  }

  /**
   * Retruns the wrapped right hand side predicate
   * @return the right hand side
   */
  public PredicateWrapper getRhs() {
    return PredicateWrapper.wrap(and.getArguments()[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AndWrapper that = (AndWrapper) o;

    return and != null ? and.equals(that.and) : that.and == null;
  }

  @Override
  public int hashCode() {
    return and != null ? and.hashCode() : 0;
  }
}
