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
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class TemporalSubsumtionTest {

  TimeSelector selector1 = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
  TimeSelector selector2 = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

  TimeLiteral lit1970a = new TimeLiteral("1970-01-01");
  TimeLiteral lit1970b = new TimeLiteral("1970-01-02");
  TimeLiteral lit2020a = new TimeLiteral("2020-05-01");
  TimeLiteral lit2020b = new TimeLiteral("2020-05-20");

  TemporalSubsumption subsumption = new TemporalSubsumption();

  @Test
  public void temporalSubsumptionTestEQ() {
    CNF cnf1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf1), cnf1);

    CNF cnf2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970b))
    );
    CNF expected2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf2), expected2);

    CNF cnf3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b))
    );
    CNF expected3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf3), expected3);

    CNF cnf4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf4), cnf4);

    CNF cnf5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LTE, lit1970b))
    );
    CNF expected5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf5), expected5);
  }

  @Test
  public void temporalSubsumptionTestNEQ() {
    CNF cnf1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b))
    );
    CNF expected1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf1), expected1);

    CNF cnf2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf2), cnf2);

    CNF cnf3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LTE, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf3), cnf3);

    CNF cnf4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf4), cnf4);
  }

  @Test
  public void temporalSubsumptionTestLTE() {
    CNF cnf1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a)),
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b))
    );
    assertEquals(subsumption.transformCNF(cnf1), cnf1);

    CNF cnf2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970b))
    );
    CNF expected2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf2), expected2);

    CNF cnf3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LTE, lit1970b))
    );
    CNF expected3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf3), expected3);

    CNF cnf4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b))
    );
    CNF expected4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf4), expected4);

    CNF cnf5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    CNF expected5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf5), expected5);
  }

  @Test
  public void temporalSubsumptionTestLT() {
    CNF cnf1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970b)),
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b))
    );
    // maybe gets resorted...
    CNF expected1 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, EQ, lit1970b)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b))
    );
    CNF result1 = subsumption.transformCNF(cnf1);
    assertTrue(result1.equals(cnf1) || result1.equals(expected1));

    CNF cnf2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970b)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf2), cnf2);

    CNF cnf3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970b))
    );
    CNF expected3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf3), expected3);

    CNF cnf4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b))
    );
    CNF expected4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf4), expected4);

    CNF cnf5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a)),
      Collections.singletonList(new Comparison(selector1, LTE, lit1970a))
    );
    CNF expected5 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf5), expected5);
  }

  @Test
  public void complexSubsumptionTest() {
    CNF cnf = Util.cnfFromLists(
      Arrays.asList(new Comparison(selector1, LT, lit1970a),
        new Comparison(selector1, NEQ, lit1970b)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Arrays.asList(new Comparison(selector2, EQ, lit2020a),
        new Comparison(selector2, LT, lit2020b),
        new Comparison(selector1, LTE, lit2020b))
    );
    CNF expected = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a))
    );
    assertEquals(subsumption.transformCNF(cnf), expected);

    CNF cnf2 = Util.cnfFromLists(
      Arrays.asList(new Comparison(selector1, LT, lit1970a),
        new Comparison(selector1, NEQ, lit1970b)),
      Collections.singletonList(new Comparison(selector1, LT, lit1970b)),
      Collections.singletonList(new Comparison(selector1, NEQ, lit1970a)),
      Arrays.asList(new Comparison(selector2, NEQ, lit2020a),
        new Comparison(selector2, LT, lit2020b))
    );
    CNF expected2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(selector1, LT, lit1970a)),
      Arrays.asList(new Comparison(selector2, NEQ, lit2020a),
        new Comparison(selector2, LT, lit2020b))
    );
    assertEquals(subsumption.transformCNF(cnf2), expected2);
  }
}
