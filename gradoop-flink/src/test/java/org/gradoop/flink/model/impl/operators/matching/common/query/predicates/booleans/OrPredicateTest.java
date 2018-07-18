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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.assertEquals;

public class OrPredicateTest {

  @Test
  public void convertToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    OrPredicate orPredicate = new OrPredicate(new Or(a,b));

    CNF reference = orPredicate.getLhs().asCNF().or(orPredicate.getRhs().asCNF());

    assertEquals(reference, orPredicate.asCNF());
  }

  @Test
  public void testGetLhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    OrPredicate orPredicate = new OrPredicate(new Or(a,b));

    assertEquals(new ComparisonExpression(a), orPredicate.getLhs());
  }

  @Test
  public void testGetRhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    OrPredicate orPredicate = new OrPredicate(new Or(a,b));

    assertEquals(new ComparisonExpression(b), orPredicate.getRhs());
  }

  protected Comparison getComparison() {
    return new Comparison(
      new Literal("a.label"),
      Comparator.EQ,
      new Literal("Person")
    );
  }
}
