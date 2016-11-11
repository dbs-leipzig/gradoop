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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .PredicateWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .expressions.ComparisonWrapper;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.*;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.assertEquals;

public class NotWrapperTest {

  @Test
  public void convertNestedNotToCnfTest() {
    Predicate a = getComparison();
    Predicate nestedNot = new Not(a);

    NotWrapper not = new NotWrapper(new Not(nestedNot));

    CNF reference = PredicateWrapper.wrap(a).asCNF();

    assertEquals(reference,not.asCNF());
  }

  @Test
  public void convertNestedComparisonToCnfTest() {
    Comparison a = getComparison();

    NotWrapper not = new NotWrapper(new Not(a));

    CNF reference = PredicateWrapper.wrap(invert(a)).asCNF();

    assertEquals(reference,not.asCNF());
  }

  @Test
  public void convertNestedAndToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    And and = new And(a,b);
    NotWrapper not = new NotWrapper(new Not(and));

    CNF reference =
      new ComparisonWrapper(invert(a)).asCNF().or(new ComparisonWrapper(invert(b)).asCNF());

    assertEquals(reference,not.asCNF());
  }

  @Test
  public void convertNestedOrToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    Or or = new Or(a,b);
    NotWrapper not = new NotWrapper(new Not(or));

    CNF reference =
      new ComparisonWrapper(invert(a)).asCNF().and(new ComparisonWrapper(invert(b)).asCNF());

    assertEquals(reference,not.asCNF());
  }

  @Test
  public void convertNestedXorToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();
    Xor xor = new Xor(a,b);
    NotWrapper not = new NotWrapper(new Not(xor));

    CNF reference =
      PredicateWrapper.wrap(new Or(new And(a,b),new And(new Not(a),new Not(b)))).asCNF();

    assertEquals(reference,not.asCNF());
  }

  protected Comparison getComparison() {
    return new Comparison(
      new Literal("a.label"),
      Comparator.EQ,
      new Literal("Person")
    );
  }

  private Comparison invert(Comparison comparison) {
    ComparableExpression[] arguments = comparison.getComparableExpressions();
    return new Comparison(
      arguments[0],
      comparison.getComparator().getInverse(),
      arguments[1]
    );
  }

}
