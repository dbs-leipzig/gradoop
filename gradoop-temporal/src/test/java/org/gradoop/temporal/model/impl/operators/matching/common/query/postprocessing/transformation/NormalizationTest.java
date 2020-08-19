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
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class NormalizationTest {

  TimeSelector ts1 = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
  TimeSelector ts2 = new TimeSelector("b", TimeSelector.TimeField.TX_TO);

  Comparison lte = new Comparison(ts1, LTE, ts2);
  Comparison lt = new Comparison(ts1, LT, ts2);
  Comparison eq = new Comparison(ts1, EQ, ts2);
  Comparison neq = new Comparison(ts1, NEQ, ts2);
  Comparison gte = new Comparison(ts1, GTE, ts2);
  Comparison gt = new Comparison(ts1, GT, ts2);

  Normalization normalization = new Normalization();

  @Test
  public void singleClauseTest() {
    // for GT or GTE, sides should be switched
    CNF gteCNF = Util.cnfFromLists(Collections.singletonList(gte));
    CNF gteExpected = Util.cnfFromLists(Collections.singletonList(gte.switchSides()));
    assertEquals(normalization.transformCNF(gteCNF), gteExpected);

    CNF gtCNF = Util.cnfFromLists(Collections.singletonList(gt));
    CNF gtExpected = Util.cnfFromLists(Collections.singletonList(gt.switchSides()));
    assertEquals(normalization.transformCNF(gtCNF), gtExpected);

    // all other comparisons should be left unchanged
    CNF eqCNF = Util.cnfFromLists(Collections.singletonList(eq));
    assertEquals(normalization.transformCNF(eqCNF), eqCNF);

    CNF neqCNF = Util.cnfFromLists(Collections.singletonList(neq));
    assertEquals(normalization.transformCNF(neqCNF), neqCNF);

    CNF ltCNF = Util.cnfFromLists(Collections.singletonList(lt));
    assertEquals(normalization.transformCNF(ltCNF), ltCNF);

    CNF lteCNF = Util.cnfFromLists(Collections.singletonList(lte));
    assertEquals(normalization.transformCNF(lteCNF), lteCNF);
  }

  @Test
  public void complexCNFTest() {
    // input
    ArrayList<Comparison> clause1 = new ArrayList<>(Arrays.asList(
      eq, gt, lte
    ));
    ArrayList<Comparison> clause2 = new ArrayList<>(Arrays.asList(
      neq, lt
    ));
    ArrayList<Comparison> clause3 = new ArrayList<>(Collections.singletonList(
      gte
    ));
    CNF input = Util.cnfFromLists(clause1, clause2, clause3);

    // expected
    ArrayList<Comparison> expectedClause1 = new ArrayList<>(Arrays.asList(
      eq, gt.switchSides(), lte
    ));
    ArrayList<Comparison> expectedClause2 = new ArrayList<>(Arrays.asList(
      neq, lt
    ));
    ArrayList<Comparison> expectedClause3 = new ArrayList<>(Collections.singletonList(
      gte.switchSides()
    ));
    CNF expected = Util.cnfFromLists(expectedClause1, expectedClause2, expectedClause3);

    assertEquals(normalization.transformCNF(input), expected);
  }
}
