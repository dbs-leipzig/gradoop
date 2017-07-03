
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
