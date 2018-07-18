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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.*;

public class ComparisonExpressionTest {

  @Test
  public void testAsCNF() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);
    ComparisonExpression comparisonExpression = new ComparisonExpression(
      new Comparison(l1, Comparator.EQ, l2)
    );

    CNFElement cnfElement = new CNFElement();
    cnfElement.addPredicate(comparisonExpression);
    CNF reference = new CNF();
    reference.addPredicate(cnfElement);

    assertEquals(reference, comparisonExpression.asCNF());
  }

  @Test
  public void testGetLhs() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);

    ComparisonExpression comparisonExpression = new ComparisonExpression(
      new Comparison(l1, Comparator.EQ, l2)
    );

    assertEquals(new LiteralComparable(l1), comparisonExpression.getLhs());
  }

  @Test
  public void testGetRhs() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);

    ComparisonExpression comparisonExpression = new ComparisonExpression(
      new Comparison(l1, Comparator.EQ, l2)
    );

    assertEquals(new LiteralComparable(l2), comparisonExpression.getRhs());
  }

  @Test
  public void testPositiveComparison() {
    Literal lhs;
    Literal rhs;

    lhs = new Literal(42);
    rhs = new Literal(42);
    assertTrue(compare(lhs, rhs, Comparator.EQ));
    assertTrue(compare(lhs, rhs, Comparator.GTE));
    assertTrue(compare(lhs, rhs, Comparator.LTE));

    lhs = new Literal(23);
    rhs = new Literal(42);
    assertTrue(compare(lhs, rhs, Comparator.NEQ));
    assertTrue(compare(lhs, rhs, Comparator.LT));
    assertTrue(compare(lhs, rhs, Comparator.LTE));

    lhs = new Literal(42);
    rhs = new Literal(23);
    assertTrue(compare(lhs, rhs, Comparator.NEQ));
    assertTrue(compare(lhs, rhs, Comparator.GT));
    assertTrue(compare(lhs, rhs, Comparator.GTE));
  }
  
  @Test
  public void testNegativeComparison() {
    Literal lhs;
    Literal rhs;
    
    lhs = new Literal(42);
    rhs = new Literal(42);
    assertFalse(compare(lhs, rhs, Comparator.NEQ));
    assertFalse(compare(lhs, rhs, Comparator.GT));
    assertFalse(compare(lhs, rhs, Comparator.LT));

    lhs = new Literal(23);
    rhs = new Literal(42);
    assertFalse(compare(lhs, rhs, Comparator.EQ));
    assertFalse(compare(lhs, rhs, Comparator.GT));
    assertFalse(compare(lhs, rhs, Comparator.GTE));

    lhs = new Literal(42);
    rhs = new Literal(23);
    assertFalse(compare(lhs, rhs, Comparator.EQ));
    assertFalse(compare(lhs, rhs, Comparator.LT));
    assertFalse(compare(lhs, rhs, Comparator.LTE));
  }

  @Test
  public void testCompareDifferentTypes() {
    Literal lhs;
    Literal rhs;

    lhs = new Literal(42);
    rhs = new Literal("42");
    assertTrue(compare(lhs, rhs, Comparator.NEQ));
    assertFalse(compare(lhs, rhs, Comparator.EQ));
    assertFalse(compare(lhs, rhs, Comparator.GT));
    assertFalse(compare(lhs, rhs, Comparator.GTE));
    assertFalse(compare(lhs, rhs, Comparator.LT));
    assertFalse(compare(lhs, rhs, Comparator.LTE));
  }

  private boolean compare(Literal lhs, Literal rhs, Comparator comparator) {
    return new ComparisonExpression(
      new Comparison(lhs,comparator,rhs)
    ).evaluate(null, null);
  }
}
