/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.CNFElementTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class CheckForCirclesTest {

  final TimeSelector sel1 = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
  final TimeSelector sel2 = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
  final TimeSelector sel3 = new TimeSelector("c", TimeSelector.TimeField.TX_TO);
  final TimeLiteral literal1 = new TimeLiteral("1970-01-01");
  final TimeLiteral literal2 = new TimeLiteral("2020-05-25");
  TimeSelector sel4 = new TimeSelector("d", VAL_TO);

  @Test(expected = QueryContradictoryException.class)
  public void checkForCirclesTest1() throws QueryContradictoryException {
    // circle with EQ and LT
    TemporalCNF cnf = cnfFromComparisons(Arrays.asList(
      Collections.singletonList(
        new Comparison(sel1, LTE, sel2)),
      Collections.singletonList(
        new Comparison(sel2, LT, sel3)),
      Collections.singletonList(
        new Comparison(sel3, EQ, sel1))));

    CheckForCircles circleCheck = new CheckForCircles();
    circleCheck.transformCNF(cnf);
  }

  @Test
  public void checkForCirclesTest2() throws QueryContradictoryException {
    // only EQ-circles
    TemporalCNF cnf = cnfFromComparisons(Arrays.asList(
      Collections.singletonList(
        new Comparison(sel1, LTE, literal1)),
      Collections.singletonList(
        new Comparison(literal1, EQ, sel3)),
      Collections.singletonList(
        new Comparison(sel3, EQ, literal2))));

    CheckForCircles circleCheck = new CheckForCircles();
    assertEquals(cnf, circleCheck.transformCNF(cnf));
  }

  @Test
  public void checkForCirclesTest3() throws QueryContradictoryException {
    // no circles
    TemporalCNF cnf = cnfFromComparisons(Arrays.asList(
      Collections.singletonList(
        new Comparison(sel1, LT, literal1)),
      Collections.singletonList(
        new Comparison(literal1, LT, sel3)),
      Collections.singletonList(
        new Comparison(sel3, LTE, literal2))));

    CheckForCircles circleCheck = new CheckForCircles();
    assertEquals(cnf, circleCheck.transformCNF(cnf));
  }

  @Test(expected = QueryContradictoryException.class)
  public void checkForCirclesTest5() throws QueryContradictoryException {
    // more than one circle
    TemporalCNF cnf = cnfFromComparisons(Arrays.asList(
      Collections.singletonList(
        new Comparison(sel1, LT, literal1)),
      Collections.singletonList(
        new Comparison(literal1, LT, sel3)),
      Collections.singletonList(
        new Comparison(sel3, LT, sel1)),
      Collections.singletonList(
        new Comparison(sel3, NEQ, sel2)),
      Collections.singletonList(
        new Comparison(sel2, LT, sel1))
    ));

    CheckForCircles circleCheck = new CheckForCircles();
    circleCheck.transformCNF(cnf);
  }

  @Test
  public void checkForCirclesTest6() throws QueryContradictoryException {
    TemporalCNF cnf = cnfFromComparisons(Arrays.asList(
      Collections.singletonList(
        new Comparison(sel1, LT, literal1)),
      Collections.singletonList(
        new Comparison(literal1, LT, sel3)),
      Collections.singletonList(
        new Comparison(sel3, LTE, literal2)),
      Arrays.asList(
        new Comparison(sel3, EQ, sel2),
        new Comparison(sel2, LT, sel1),
        new Comparison(sel1, LTE, sel3)
      )));
    CheckForCircles circleCheck = new CheckForCircles();
    assertEquals(circleCheck.transformCNF(cnf), cnf);
  }

  private TemporalCNF cnfFromComparisons(List<List<Comparison>> comparisons) {
    TemporalCNF cnf = new TemporalCNF();
    for (List<Comparison> clause : comparisons) {
      ArrayList<ComparisonExpressionTPGM> wrappedClause = new ArrayList<>();
      for (Comparison comparison : clause) {
        wrappedClause.add(new ComparisonExpressionTPGM(comparison));
      }
      CNFElementTPGM cnfClause = new CNFElementTPGM(wrappedClause);
      cnf = cnf.and(new TemporalCNF(Collections.singletonList(cnfClause)));
    }
    return cnf;
  }
}
