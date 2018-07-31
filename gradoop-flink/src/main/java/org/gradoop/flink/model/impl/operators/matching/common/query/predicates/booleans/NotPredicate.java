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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

/**
 * Wraps a {@link org.s1ck.gdl.model.predicates.booleans.Not} predicate
 */
public class NotPredicate extends QueryPredicate {

  /**
   * Holds the wrapped not predicate
   */
  private final Not not;

  /**
   * Create a new wrapper
   * @param not the wrapped not predicate
   */
  public NotPredicate(Not not) {
    this.not = not;
  }

  /**
   * Converts the predicate into conjunctive normal form
   * @return predicate in cnf
   */
  @Override
  public CNF asCNF() {
    Predicate expression = not.getArguments()[0];

    if (expression.getClass() == Comparison.class) {
      CNF cnf = new CNF();
      CNFElement cnfElement = new CNFElement();
      cnfElement.addPredicate(new ComparisonExpression(invertComparison((Comparison) expression)));
      cnf.addPredicate(cnfElement);
      return cnf;

    } else if (expression.getClass() == Not.class) {
      return QueryPredicate.createFrom(expression.getArguments()[0]).asCNF();

    } else if (expression.getClass() == And.class) {
      Predicate[] otherArguments = expression.getArguments();
      Or or = new Or(
        new Not(otherArguments[0]),
        new Not(otherArguments[0])
      );
      return QueryPredicate.createFrom(or).asCNF();

    } else if (expression.getClass() == Or.class) {
      Predicate[] otherArguments = expression.getArguments();
      And and = new And(
        new Not(otherArguments[0]),
        new Not(otherArguments[1])
      );

      return QueryPredicate.createFrom(and).asCNF();

    } else {
      Predicate[] otherArguments = expression.getArguments();
      Or or = new Or(
        new And(
          otherArguments[0],
          otherArguments[1]),
        new And(
          new Not(otherArguments[0]),
          new Not(otherArguments[0]))
      );

      return QueryPredicate.createFrom(or).asCNF();
    }
  }

  /**
   * Invert a comparison
   * eg NOT(a > b) == (a <= b)
   * @param comparison the comparison that will be inverted
   * @return inverted comparison
   */
  private Comparison invertComparison(
    Comparison comparison) {
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

    NotPredicate that = (NotPredicate) o;

    return not != null ? not.equals(that.not) : that.not == null;

  }

  @Override
  public int hashCode() {
    return not != null ? not.hashCode() : 0;
  }
}
