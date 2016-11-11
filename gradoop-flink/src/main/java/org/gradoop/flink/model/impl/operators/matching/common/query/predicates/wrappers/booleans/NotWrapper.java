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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.PredicateWrapper;


import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .expressions.ComparisonWrapper;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

/**
 * Wraps a {@link Not} predicate
 */
public class NotWrapper extends PredicateWrapper{

  /**
   * Holds the wrapped not predicate
   */
  private final Not not;

  /**
   * Create a new wrapper
   * @param not the wrapped not predicate
   */
  public NotWrapper(Not not) {
    this.not = not;
  }

  /**
   * Converts the predicate into conjunctive normal form
   * @return predicate in cnf
   */
  @Override
  public CNF asCNF() {
    Predicate expression = not.getArguments()[0];

    if(expression.getClass() == Comparison.class) {
      CNF CNF = new CNF();
      CNFElement CNFElement = new CNFElement();

      CNFElement.addPredicate(new ComparisonWrapper(invertComparison((Comparison) expression)));
      CNF.addPredicate(CNFElement);
      return CNF;
    }

    else if (expression.getClass() == Not.class) {
      return PredicateWrapper.wrap(expression.getArguments()[0]).asCNF();
    }

    else if (expression.getClass() == And.class) {
      Predicate[] otherArguments = expression.getArguments();
      Or or = new Or(
        new Not(otherArguments[0]),
        new Not(otherArguments[0])
      );
      return PredicateWrapper.wrap(or).asCNF();
    }

    else if (expression.getClass() == Or.class) {
      Predicate[] otherArguments = expression.getArguments();
      And and = new And(
        new Not(otherArguments[0]),
        new Not(otherArguments[0])
      );

      return PredicateWrapper.wrap(and).asCNF();
    }

    else {
      Predicate[] otherArguments = expression.getArguments();
      Or or = new Or(
        new And(
          otherArguments[0],
          otherArguments[1]),
        new And(
          new Not(otherArguments[0]),
          new Not(otherArguments[0]))
      );

      return PredicateWrapper.wrap(or).asCNF();
    }
  }

  /**
   * Invert a comparison
   * eg NOT(a > b) == (a <= b)
   * @param comparison the comparison that will be inverted
   * @return inverted comparison
   */
  private Comparison invertComparison(Comparison comparison) {
    ComparableExpression[] arguments = comparison.getComparableExpressions();
    Comparator op = comparison.getComparator();

    return new Comparison(
      arguments[0],
      op.getInverse(),
      arguments[1]
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NotWrapper that = (NotWrapper) o;

    return not != null ? not.equals(that.not) : that.not == null;

  }

  @Override
  public int hashCode() {
    return not != null ? not.hashCode() : 0;
  }
}
