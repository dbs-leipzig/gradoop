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
  .expressions.ComparisonWrapper;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.assertEquals;

public class XorWrapperTest {

  @Test
  public void convertToCnfTest() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    XorWrapper xor = new XorWrapper(new Xor(a,b));

    CNF reference = new OrWrapper(new Or(new And(a,new Not(b)),new And(new Not(a),b))).asCNF();

    assertEquals(reference,xor.asCNF());
  }

  @Test
  public void testGetLhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    OrWrapper or = new OrWrapper(new Or(a,b));

    assertEquals(new ComparisonWrapper(a), or.getLhs());
  }

  @Test
  public void testGetRhs() {
    Comparison a = getComparison();
    Comparison b = getComparison();

    OrWrapper or = new OrWrapper(new Or(a,b));

    assertEquals(new ComparisonWrapper(b), or.getRhs());
  }

  protected Comparison getComparison() {
    return new Comparison(
      new Literal("a.label"),
      Comparator.EQ,
      new Literal("Person")
    );
  }
}
