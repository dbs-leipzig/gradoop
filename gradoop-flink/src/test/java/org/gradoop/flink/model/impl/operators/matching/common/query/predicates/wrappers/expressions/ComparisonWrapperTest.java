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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.expressions;


import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .comparables.LiteralWrapper;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ComparisonWrapperTest {

  @Test
  public void testAsCNF() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);
    ComparisonWrapper comparison = new ComparisonWrapper(new Comparison(l1, Comparator.EQ, l2));

    CNFElement cnfElement = new CNFElement();
    cnfElement.addPredicate(comparison);
    CNF reference = new CNF();
    reference.addPredicate(cnfElement);

    assertEquals(reference, comparison.asCNF());
  }

  @Test
  public void testGetLhs() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);

    ComparisonWrapper comparison = new ComparisonWrapper(new Comparison(l1, Comparator.EQ, l2));

    assertEquals(new LiteralWrapper(l1), comparison.getLhs());
  }

  @Test
  public void testGetRhs() {
    Literal l1 = new Literal(1);
    Literal l2 = new Literal(1);

    ComparisonWrapper comparison = new ComparisonWrapper(new Comparison(l1, Comparator.EQ, l2));

    assertEquals(new LiteralWrapper(l2), comparison.getRhs());
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

    GradoopId id = GradoopId.get();
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
    return new ComparisonWrapper(
      new Comparison(lhs,comparator,rhs)
    ).evaluate(new HashMap<>());
  }
}
