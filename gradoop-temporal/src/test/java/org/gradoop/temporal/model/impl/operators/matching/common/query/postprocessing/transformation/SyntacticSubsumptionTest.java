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
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class SyntacticSubsumptionTest {
  TimeSelector ts1 = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
  TimeSelector ts2 = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

  TimeLiteral tl1 = new TimeLiteral("now");

  Comparison lte1 = new Comparison(ts1, LTE, ts2);
  Comparison lt1 = new Comparison(ts1, LT, ts2);
  Comparison eq = new Comparison(ts1, EQ, ts2);
  Comparison neq = new Comparison(ts1, NEQ, ts2);

  Comparison withLiteral = new Comparison(ts1, LTE, tl1);

  Comparison lte2 = new Comparison(ts2, LTE, ts1);
  Comparison lt2 = new Comparison(ts2, LT, ts1);

  SyntacticSubsumption subsumer = new SyntacticSubsumption();

  @Test
  public void syntacticSubsumtionTest() {
    CNF cnf1 = Util.cnfFromLists(
      //subsumed by (eq)
      Arrays.asList(lte1, eq, lt2),
      Collections.singletonList(eq),
      // subsumed by (eq)
      Arrays.asList(lt1, eq),
      Arrays.asList(lte1, lt2)
    );
    CNF expected1 = Util.cnfFromLists(
      Collections.singletonList(eq),
      Arrays.asList(lte1, lt2)
    );
    assertEquals(subsumer.transformCNF(cnf1), expected1);
  }

  @Test
  public void syntacticSubsumtionTest2() {

    CNF cnf2 = Util.cnfFromLists(
      //subsumed by (eq, lt2)
      Arrays.asList(lte1, eq, lt2),
      Arrays.asList(eq, lt2)
    );
    CNF expected2 = Util.cnfFromLists(
      Arrays.asList(eq, lt2)
    );
    assertEquals(subsumer.transformCNF(cnf2), expected2);
  }

  @Test
  public void syntacticSubsumtionsTest3() {
    // no subsumtions here
    CNF cnf3 = Util.cnfFromLists(
      Arrays.asList(lte1, eq, lt2),
      Arrays.asList(lt1, eq)
    );
    // gets resorted...
    CNF expected3 = Util.cnfFromLists(
      Arrays.asList(lt1, eq),
      Arrays.asList(lte1, eq, lt2)
    );
    assertEquals(subsumer.transformCNF(cnf3), expected3);
  }

  @Test
  public void syntacticSubsumtionTest4() {

    CNF cnf4 = Util.cnfFromLists(
      //subsumed by (eq, lt2)
      Arrays.asList(lte1, eq, lt2, lte1),
      Arrays.asList(eq, lt2, eq)
    );
    CNF expected4 = Util.cnfFromLists(
      Arrays.asList(eq, lt2)
    );
    assertEquals(subsumer.transformCNF(cnf4), expected4);
  }

  @Test
  public void syntacticSubsumtionTest5() {

    CNF cnf5 = Util.cnfFromLists(
      //subsumed by (eq, lt2)
      Collections.singletonList(withLiteral),
      Collections.singletonList(withLiteral)
    );
    CNF expected5 = Util.cnfFromLists(
      Collections.singletonList(withLiteral)
    );
    assertEquals(subsumer.transformCNF(cnf5), expected5);
  }
}
