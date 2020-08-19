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
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class MinMaxUnfoldingTest {

  TimeSelector ts1 = new TimeSelector("a", TX_FROM);
  TimeSelector ts2 = new TimeSelector("a", TX_TO);
  TimeSelector ts3 = new TimeSelector("a", VAL_FROM);
  TimeLiteral l1 = new TimeLiteral("1970-01-01");
  TimeLiteral l2 = new TimeLiteral("2020-05-24");

  MinTimePoint min1 = new MinTimePoint(ts1, ts2, ts3);
  MinTimePoint min2 = new MinTimePoint(ts3, l1);

  MaxTimePoint max1 = new MaxTimePoint(l2, ts1, ts2);
  MaxTimePoint max2 = new MaxTimePoint(ts2, l1);

  MinMaxUnfolding unfolder = new MinMaxUnfolding();

  @Test
  public void minMaxUnfoldingTest() {
    Comparison c1 = new Comparison(min1, Comparator.LT, l1);
    CNF cnf1 = Util.cnfFromLists(
      Collections.singletonList(c1)
    );
    CNF expected1 = Util.cnfFromLists(
      Arrays.asList(
        new Comparison(min1.getArgs().get(0), Comparator.LT, l1),
        new Comparison(min1.getArgs().get(1), Comparator.LT, l1),
        new Comparison(min1.getArgs().get(2), Comparator.LT, l1)
      )
    );
    assertEquals(unfolder.transformCNF(cnf1), expected1);


    Comparison c2 = new Comparison(max1, LTE, l1);
    CNF cnf2 = Util.cnfFromLists(
      Collections.singletonList(c2)
    );
    CNF expected2 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(max1.getArgs().get(0), LTE, l1)),
      Collections.singletonList(new Comparison(max1.getArgs().get(1), LTE, l1)),
      Collections.singletonList(new Comparison(max1.getArgs().get(2), LTE, l1))

    );
    assertEquals(unfolder.transformCNF(cnf2), expected2);


    Comparison c3 = new Comparison(max1, Comparator.EQ, l1);
    CNF cnf3 = Util.cnfFromLists(
      Collections.singletonList(c3)
    );
    CNF expected3 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(max1, EQ, l1)));
    assertEquals(unfolder.transformCNF(cnf3), expected3);


    Comparison c4 = new Comparison(l1, LTE, min1);
    CNF cnf4 = Util.cnfFromLists(
      Collections.singletonList(c4)
    );
    CNF expected4 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(l1, LTE, min1.getArgs().get(0))),
      Collections.singletonList(new Comparison(l1, LTE, min1.getArgs().get(1))),
      Collections.singletonList(new Comparison(l1, LTE, min1.getArgs().get(2)))
    );
    assertEquals(unfolder.transformCNF(cnf4), expected4);


    Comparison c5 = new Comparison(l1, Comparator.LT, max1);
    CNF cnf5 = Util.cnfFromLists(
      Collections.singletonList(c5)
    );
    CNF expected5 = Util.cnfFromLists(
      Arrays.asList(
        new Comparison(l1, Comparator.LT, max1.getArgs().get(0)),
        new Comparison(l1, Comparator.LT, max1.getArgs().get(1)),
        new Comparison(l1, Comparator.LT, max1.getArgs().get(2))
      )
    );
    assertEquals(unfolder.transformCNF(cnf5), expected5);


    Comparison c6 = new Comparison(l1, NEQ, min1);
    CNF cnf6 = Util.cnfFromLists(
      Collections.singletonList(c6)
    );
    CNF expected6 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(l1, NEQ, min1)));
    assertEquals(unfolder.transformCNF(cnf6), expected6);


    // MIN < MIN is only unfolded once, rhs stays untouched (to ensure a CNF result)
    Comparison c7 = new Comparison(min2, Comparator.LT, min2);
    CNF cnf7 = Util.cnfFromLists(
      Collections.singletonList(c7)
    );
    CNF expected7 = Util.cnfFromLists(
      Arrays.asList(
        new Comparison(min2.getArgs().get(0), Comparator.LT, min2),
        new Comparison(min2.getArgs().get(1), Comparator.LT, min2)
      ));
    assertEquals(unfolder.transformCNF(cnf7), expected7);

    // MIN < MAX
    Comparison c8 = new Comparison(min2, LTE, max2);
    CNF cnf8 = Util.cnfFromLists(
      Collections.singletonList(c8)
    );
    CNF expected8 = Util.cnfFromLists(
      Arrays.asList(
        new Comparison(min2.getArgs().get(0), LTE, max2.getArgs().get(0)),
        new Comparison(min2.getArgs().get(0), LTE, max2.getArgs().get(1)),
        new Comparison(min2.getArgs().get(1), LTE, max2.getArgs().get(0)),
        new Comparison(min2.getArgs().get(1), LTE, max2.getArgs().get(1))
      ));
    assertEquals(unfolder.transformCNF(cnf8), expected8);

    // MAX < MIN
    Comparison c9 = new Comparison(max2, LTE, min2);
    CNF cnf9 = Util.cnfFromLists(
      Collections.singletonList(c9)
    );
    CNF expected9 = Util.cnfFromLists(
      Collections.singletonList(new Comparison(max2.getArgs().get(0), LTE, min2.getArgs().get(0))),
      Collections.singletonList(new Comparison(max2.getArgs().get(0), LTE, min2.getArgs().get(1))),
      Collections.singletonList(new Comparison(max2.getArgs().get(1), LTE, min2.getArgs().get(0))),
      Collections.singletonList(new Comparison(max2.getArgs().get(1), LTE, min2.getArgs().get(1)))
    );
    assertEquals(unfolder.transformCNF(cnf9), expected9);

    // MAX < MAX
    Comparison c10 = new Comparison(max2, LTE, max2);
    CNF cnf10 = Util.cnfFromLists(
      Collections.singletonList(c10)
    );
    CNF expected10 = Util.cnfFromLists(
      Arrays.asList(new Comparison(max2.getArgs().get(0), LTE, max2.getArgs().get(0)),
        new Comparison(max2.getArgs().get(0), LTE, max2.getArgs().get(1))),
      Arrays.asList(new Comparison(max2.getArgs().get(1), LTE, max2.getArgs().get(0)),
        new Comparison(max2.getArgs().get(1), LTE, max2.getArgs().get(1)))
    );
    assertEquals(unfolder.transformCNF(cnf10), expected10);

    Comparison c11 = new Comparison(max2, NEQ, min2);
    CNF cnf11 = Util.cnfFromLists(
      Collections.singletonList(c11)
    );
    CNF expected11 = Util.cnfFromLists(
      Collections.singletonList(c11)
    );
    assertEquals(unfolder.transformCNF(cnf11), expected11);

    // more complex
    CNF noMinMax = Util.cnfFromLists(
      Arrays.asList(
        new Comparison(l1, LTE, l2),
        new Comparison(l1, GTE, l2)
      ),
      Collections.singletonList(
        new Comparison(ts1, NEQ, ts2)
      )
    );
    CNF complex = cnf1.and(cnf2)
      .and(cnf3).and(cnf4).and(noMinMax)
      .and(cnf5).and(cnf6).and(cnf7).and(cnf8)
      .and(cnf9).and(cnf10).and(cnf11);
    CNF expectedComplex = expected1.and(expected2).and(expected3)
      .and(expected4).and(noMinMax).and(expected5).and(expected6).and(expected7)
      .and(expected8).and(expected9).and(expected10).and(expected11);
    assertEquals(unfolder.transformCNF(complex), expectedComplex);

    CNF empty = new CNF();
    assertEquals(unfolder.transformCNF(empty), empty);
  }
}
