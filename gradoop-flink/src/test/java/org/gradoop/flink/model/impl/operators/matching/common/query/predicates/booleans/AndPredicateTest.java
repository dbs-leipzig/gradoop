
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.assertEquals;

public class AndPredicateTest {

  @Test
  public void convertToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    AndPredicate andPredicate = new AndPredicate(new And(a,b));

    CNF reference = andPredicate.getLhs().asCNF().and(andPredicate.getRhs().asCNF());

    assertEquals(reference, andPredicate.asCNF());
  }

  @Test
  public void testGetLhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    AndPredicate andPredicate = new AndPredicate(new And(a,b));

    assertEquals(new ComparisonExpression(a), andPredicate.getLhs());
  }

  @Test
  public void testGetRhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    AndPredicate andPredicate = new AndPredicate(new And(a,b));

    assertEquals(new ComparisonExpression(b), andPredicate.getRhs());
  }

  protected Comparison getComparison() {
    return new Comparison(
      new Literal("a.label"),
      Comparator.EQ,
      new Literal("Person")
    );
  }
}
