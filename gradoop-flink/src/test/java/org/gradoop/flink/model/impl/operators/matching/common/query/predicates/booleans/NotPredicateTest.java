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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.assertEquals;

public class NotPredicateTest {

  @Test
  public void convertNestedNotToCnfTest() {
    Predicate a = getComparison();
    Predicate nestedNot = new Not(a);

    NotPredicate notPredicate = new NotPredicate(new Not(nestedNot));

    CNF reference = QueryPredicate.createFrom(a).asCNF();

    assertEquals(reference, notPredicate.asCNF());
  }

  @Test
  public void convertNestedComparisonToCnfTest() {
    Comparison a = getComparison();

    NotPredicate notPredicate = new NotPredicate(new Not(a));

    CNF reference = QueryPredicate.createFrom(invert(a)).asCNF();

    assertEquals(reference, notPredicate.asCNF());
  }

  @Test
  public void convertNestedAndToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    And and = new And(a,b);
    NotPredicate notPredicate = new NotPredicate(new Not(and));

    CNF reference =
      new ComparisonExpression(invert(a)).asCNF().or(new ComparisonExpression(invert(b)).asCNF());

    assertEquals(reference, notPredicate.asCNF());
  }

  @Test
  public void convertNestedOrToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    Or or = new Or(a,b);
    NotPredicate notPredicate = new NotPredicate(new Not(or));

    CNF reference =
      new ComparisonExpression(invert(a)).asCNF().and(new ComparisonExpression(invert(b)).asCNF());

    assertEquals(reference, notPredicate.asCNF());
  }

  @Test
  public void convertNestedXorToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    Xor xor = new Xor(a,b);
    NotPredicate notPredicate = new NotPredicate(new Not(xor));

    CNF reference =
      QueryPredicate.createFrom(
        new Or(
          new And(a,b),
          new And(
            new Not(a),
            new Not(b)
          )
        )
      ).asCNF();

    assertEquals(reference, notPredicate.asCNF());
  }

  protected Comparison getComparison() {
    return new Comparison(
      new Literal("a.label"),
      Comparator.EQ,
      new Literal("Person")
    );
  }

  private Comparison invert(
    Comparison comparison) {
    ComparableExpression[] arguments = comparison.getComparableExpressions();
    return new Comparison(
      arguments[0],
      comparison.getComparator().getInverse(),
      arguments[1]
    );
  }

}
