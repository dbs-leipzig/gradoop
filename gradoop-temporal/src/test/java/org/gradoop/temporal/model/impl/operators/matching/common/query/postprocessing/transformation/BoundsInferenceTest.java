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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class BoundsInferenceTest {

  TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
  TimeSelector aTxFrom = new TimeSelector("a", TX_FROM);
  TimeSelector bTxFrom = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
  TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
  TimeSelector cValTo = new TimeSelector("c", VAL_TO);
  TimeSelector cTxTo = new TimeSelector("c", TX_TO);

  TimeLiteral literal1970 = new TimeLiteral("1970-01-01");
  TimeLiteral literal2020 = new TimeLiteral("2020-05-25");

  @Test
  public void preprocessingTest1() throws QueryContradictoryException {
    // 1970-01-01 < c.val_to <= b.tx_from < a.val_from < 2020-05-25
    // => lower(c.val_to) = 1L, upper(c.val_to) = ms(2020-05-25)-2
    // => lower(b.tx_from) = 1L, upper(b.tx_from) = ms(2020-05-25)-2
    // => lower(a.val_from) = 2L, upper(a.val_from) = ms(2020-05-25)-1

    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(bTxFrom, LT, aValFrom)),
      Collections.singletonList(new Comparison(cValTo, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(literal1970, LT, cValTo)),
      Collections.singletonList(new Comparison(aValFrom, LT, literal2020))
    );
    BoundsInference prep = new BoundsInference();
    CNF processedCNF = prep.transformCNF(cnf);

    long l2020 = literal2020.getMilliseconds();

    CNF expectedCNF = Util.cnfFromLists(
      Collections.singletonList(new Comparison(bTxFrom, LT, aValFrom)),
      Collections.singletonList(new Comparison(cValTo, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(
        new TimeLiteral(1), LTE, cValTo)),
      Collections.singletonList(new Comparison(
        cValTo, LTE, new TimeLiteral(l2020 - 1))),
      Collections.singletonList(new Comparison(
        new TimeLiteral(1), LTE, bTxFrom)),
      Collections.singletonList(new Comparison(
        bTxFrom, LTE, new TimeLiteral(l2020 - 1))),
      Collections.singletonList(new Comparison(
        new TimeLiteral(1), LTE, aValFrom)),
      Collections.singletonList(new Comparison(
        aValFrom, LTE, new TimeLiteral(l2020 - 1)))
    );

    compareCNFs(expectedCNF, processedCNF);
  }

  private void compareCNFs(CNF expectedCNF, CNF processedCNF) {
    assertEquals(expectedCNF.size(), processedCNF.size());
    for (CNFElement c1 : expectedCNF.getPredicates()) {
      List<ComparisonExpression> c1Comps = c1.getPredicates();
      c1Comps.sort(Comparator.comparingInt(ComparisonExpression::hashCode));
      boolean found = false;
      for (CNFElement c2 : processedCNF.getPredicates()) {
        List<ComparisonExpression> c2Comps = c2.getPredicates();
        c2Comps.sort(Comparator.comparingInt(ComparisonExpression::hashCode));
        if (c2Comps.equals(c1Comps)) {
          found = true;
          break;
        }
      }
      if (!found) {
        fail();
      }
    }
  }

  @Test
  public void preprocessingTest2() throws QueryContradictoryException {
    // a.tx_from = b.tx_from = 1970-01-01
    // => lower(a.tx_from) = upper(a.tx_from) = lower(b.tx_from)
    //      = lower(b.tx_to) = 0L
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxTo, EQ, bTxFrom)),
      Collections.singletonList(new Comparison(bTxFrom, EQ, literal1970))
    );
    BoundsInference prep = new BoundsInference();

    CNF processedCNF = prep.transformCNF(cnf);

    CNF expectedCNF = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxTo, EQ, bTxFrom)),
      Collections.singletonList(new Comparison(aTxTo, EQ, literal1970)),
      Collections.singletonList(new Comparison(bTxFrom, EQ, literal1970))
    );

    compareCNFs(processedCNF, expectedCNF);

  }

  @Test
  public void preprocessingTest3() throws QueryContradictoryException {
    // a.tx_from < b.tx_from, 1970-01-01 <= b.tx_from, b.tx_from <= 2020-05-25
    // => lower(b.tx_from) = 1970-01-01, upper(b.tx-from)=2020-05-25 = upper(a.tx-from)
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LT, bTxFrom)),
      Collections.singletonList(new Comparison(literal1970, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, literal2020))
    );

    BoundsInference prep = new BoundsInference();
    CNF processedCNF = prep.transformCNF(cnf);

    long l2020 = literal2020.getMilliseconds();

    CNF expectedCNF = Util.cnfFromLists(
      Collections.singletonList(new Comparison(
        aTxFrom, LT, bTxFrom)),
      Collections.singletonList(new Comparison(
        aTxFrom, LTE, new TimeLiteral(l2020 - 1))),
      Collections.singletonList(new Comparison(
        literal1970, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(
        bTxFrom, LTE, new TimeLiteral(l2020)))
    );

    compareCNFs(expectedCNF, processedCNF);
  }

  @Test(expected = QueryContradictoryException.class)
  public void preprocessingTest4() throws QueryContradictoryException {
    // obvious contradiction (should not occur in reality, as
    // a CheckForCircles is done before this processing step)
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LT, literal1970)),
      Collections.singletonList(new Comparison(literal2020, LTE, aTxFrom))
    );
    BoundsInference prep = new BoundsInference();
    prep.transformCNF(cnf);
  }

  @Test(expected = QueryContradictoryException.class)
  public void preprocessingTest5() throws QueryContradictoryException {
    // obvious contradiction (should not occur in reality, as
    // a CheckForCircles is done before this processing step)
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LT, literal1970)),
      Collections.singletonList(new Comparison(literal1970, EQ, aTxFrom))
    );
    BoundsInference prep = new BoundsInference();
    prep.transformCNF(cnf);
  }

  @Test(expected = QueryContradictoryException.class)
  public void preprocessingTest6() throws QueryContradictoryException {
    // obvious contradiction (should not occur in reality, as
    // a CheckForCircles is done before this processing step)
    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(literal1970, EQ, aTxFrom)),
      Collections.singletonList(new Comparison(aValFrom, EQ, aTxFrom)),
      Collections.singletonList(new Comparison(aValFrom, NEQ, literal1970))
    );
    BoundsInference prep = new BoundsInference();
    prep.transformCNF(cnf);
  }

  // trivial bounds
  @Test
  public void preprocessingTest7() throws QueryContradictoryException {
    // 1970-01-01 < c.val_to <= b.tx_from < a.val_from < 2020-05-25
    // => lower(c.val_to) = 1L, upper(c.val_to) = ms(2020-05-25)-2
    // => lower(b.tx_from) = 1L, upper(b.tx_from) = ms(2020-05-25)-2
    // => lower(a.val_from) = 2L, upper(a.val_from) = ms(2020-05-25)-1

    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, aTxTo)),
      Collections.singletonList(new Comparison(aTxTo, LTE, literal2020))
    );
    BoundsInference prep = new BoundsInference();
    CNF processedCNF = prep.transformCNF(cnf);

    long l2020 = literal2020.getMilliseconds();

    CNF expectedCNF = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, aTxTo)),
      Collections.singletonList(new Comparison(aTxTo, LTE, literal2020)),
      Collections.singletonList(new Comparison(bTxFrom, LTE, literal2020))
    );

    compareCNFs(expectedCNF, processedCNF);
  }

  // redundancies
  @Test
  public void preprocessingTest8() throws QueryContradictoryException {
    // 1970-01-01 < c.val_to <= b.tx_from < a.val_from < 2020-05-25
    // => lower(c.val_to) = 1L, upper(c.val_to) = ms(2020-05-25)-2
    // => lower(b.tx_from) = 1L, upper(b.tx_from) = ms(2020-05-25)-2
    // => lower(a.val_from) = 2L, upper(a.val_from) = ms(2020-05-25)-1

    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, bTxFrom)),
      Collections.singletonList(new Comparison(aTxFrom, LTE, aTxTo)),
      Collections.singletonList(new Comparison(literal1970, LT, literal2020))
    );
    BoundsInference prep = new BoundsInference();
    CNF processedCNF = prep.transformCNF(cnf);


    CNF expectedCNF = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, LTE, bTxFrom))
    );

    compareCNFs(expectedCNF, processedCNF);
  }

  // NEQ contradictions
  @Test(expected = QueryContradictoryException.class)
  public void preprocessingTest9() throws QueryContradictoryException {
    // 1970-01-01 < c.val_to <= b.tx_from < a.val_from < 2020-05-25
    // => lower(c.val_to) = 1L, upper(c.val_to) = ms(2020-05-25)-2
    // => lower(b.tx_from) = 1L, upper(b.tx_from) = ms(2020-05-25)-2
    // => lower(a.val_from) = 2L, upper(a.val_from) = ms(2020-05-25)-1

    CNF cnf = Util.cnfFromLists(
      Collections.singletonList(new Comparison(aTxFrom, EQ, bTxFrom)),
      Collections.singletonList(new Comparison(bTxFrom, NEQ, literal2020)),
      Collections.singletonList(new Comparison(aTxFrom, EQ, literal2020))
    );
    BoundsInference prep = new BoundsInference();
    CNF processedCNF = prep.transformCNF(cnf);
  }

}
