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
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class TrivialTautologiesTest {

  TimeSelector aTxFrom = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
  TimeSelector aTxTo = new TimeSelector("a", TimeSelector.TimeField.TX_TO);
  TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
  TimeSelector aValTo = new TimeSelector("a", VAL_TO);
  Comparison tsTaut1 = new Comparison(aTxFrom, LTE, aTxTo);
  Comparison tsTaut2 = new Comparison(aValFrom, LTE, aValTo);
  Comparison tsTaut3 = new Comparison(aValTo, EQ, aValTo);
  Comparison tsNonTaut1 = new Comparison(aTxFrom, NEQ, aTxTo);
  Comparison tsNonTaut2 = new Comparison(aValFrom, LT, aValTo);

  TimeLiteral tl1 = new TimeLiteral("1970-01-01");
  TimeLiteral tl2 = new TimeLiteral("2020-05-23");
  Comparison tlTaut1 = new Comparison(tl1, LT, tl2);
  Comparison tlTaut2 = new Comparison(tl1, LTE, tl2);
  Comparison tlTaut3 = new Comparison(tl2, EQ, tl2);
  Comparison tlTaut4 = new Comparison(tl1, NEQ, tl2);
  Comparison tlNonTaut1 = new Comparison(tl1, EQ, tl2);
  Comparison tlNonTaut2 = new Comparison(tl2, LT, tl1);

  Literal l1 = new Literal(1);
  Literal l2 = new Literal(3);
  Comparison lTaut1 = new Comparison(l1, LT, l2);
  Comparison lTaut2 = new Comparison(l1, LTE, l2);
  Comparison lTaut3 = new Comparison(l2, EQ, l2);
  Comparison lTaut4 = new Comparison(l1, NEQ, l2);
  Comparison lNonTaut1 = new Comparison(l1, EQ, l2);
  Comparison lNonTaut2 = new Comparison(l2, LT, l1);

  TrivialTautologies tautologyDetector = new TrivialTautologies();

  @Test
  public void trivialTautologiesTest() {
    CNF cnf1 = Util.cnfFromLists(
      Arrays.asList(tsTaut1, lNonTaut1, tsNonTaut2),
      Collections.singletonList(lNonTaut2),
      Arrays.asList(lTaut1, lTaut4),
      Arrays.asList(lTaut4, lNonTaut2),
      Collections.singletonList(tlTaut1),
      Collections.singletonList(tlTaut2),
      Arrays.asList(tlTaut3, tsNonTaut1),
      Arrays.asList(tlTaut4, tlNonTaut1, tsTaut2),
      Arrays.asList(tsTaut2, tsTaut3),
      Collections.singletonList(tsTaut3),
      Arrays.asList(tlNonTaut2, lNonTaut2),
      Arrays.asList(tlNonTaut2, lTaut1),
      Arrays.asList(tlNonTaut2, lTaut2),
      Collections.singletonList(lTaut3)
    );

    CNF expected1 = Util.cnfFromLists(
      Collections.singletonList(lNonTaut2),
      Arrays.asList(tlNonTaut2, lNonTaut2)
    );

    assertEquals(tautologyDetector.transformCNF(cnf1), expected1);

    // only tautologies here
    CNF cnf2 = Util.cnfFromLists(
      Arrays.asList(tsTaut1, lTaut3)
    );
    CNF expected2 = new CNF();
    assertEquals(tautologyDetector.transformCNF(cnf2), expected2);


  }

}
