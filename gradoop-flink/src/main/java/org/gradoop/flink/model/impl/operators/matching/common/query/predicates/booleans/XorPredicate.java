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
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;

/**
 * Wraps a {@link org.s1ck.gdl.model.predicates.booleans.Xor} predicate
 */
public class XorPredicate extends QueryPredicate {
  /**
   * Holdes the wrapped predicate
   */
  private final Xor xor;

  /**
   * Creates a new Wrapper
   *
   * @param xor The wrapped xor predicate
   */
  public XorPredicate(Xor xor) {
    this.xor = xor;
  }

  /**
   * Converts the predicate into conjunctive normal form
   *
   * @return predicate in cnf
   */
  public CNF asCNF() {
    Predicate lhs = xor.getArguments()[0];
    Predicate rhs = xor.getArguments()[1];

    QueryPredicate wrapper = QueryPredicate.createFrom(
      new Or(new And(lhs, new Not(rhs)), new And(new Not(lhs), rhs))
    );

    return wrapper.asCNF();
  }

  /**
   * Returns the wrapped left hand side predicate
   * @return wrapped left hand side predicate
   */
  public QueryPredicate getLhs() {
    return QueryPredicate.createFrom(xor.getArguments()[0]);
  }

  /**
   * Returns the wrapped right hand side predicate
   * @return wrapped right hand side predicate
   */
  public QueryPredicate getRhs() {
    return QueryPredicate.createFrom(xor.getArguments()[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    XorPredicate that = (XorPredicate) o;

    return xor != null ? xor.equals(that.xor) : that.xor == null;

  }

  @Override
  public int hashCode() {
    return xor != null ? xor.hashCode() : 0;
  }
}
