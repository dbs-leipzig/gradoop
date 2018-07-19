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
