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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.relationgraph;

import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class RelationGraphTest {
  TimeSelector sel1 = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
  TimeSelector sel2 = new TimeSelector("b", TimeSelector.TimeField.TX_FROM);
  TimeSelector sel3 = new TimeSelector("c", TimeSelector.TimeField.TX_TO);
  TimeSelector sel4 = new TimeSelector("d", VAL_TO);

  TimeLiteral literal1 = new TimeLiteral("1970-01-01");
  TimeLiteral literal2 = new TimeLiteral("2020-05-25");

  @Test
  public void cycleTest1() {
    ArrayList<ComparisonExpressionTPGM> comps = new ArrayList<>(
      Arrays.asList(
        new ComparisonExpressionTPGM(
          new Comparison(sel1, LTE, sel2)),
        new ComparisonExpressionTPGM(
          new Comparison(sel2, LT, sel3)),
        new ComparisonExpressionTPGM(
          new Comparison(sel3, EQ, sel1))
      )
    );
    RelationGraph graph = new RelationGraph(new HashSet<>(comps));
    List<List<Comparator>> circles = graph.findAllCircles();
    assertEquals(circles.size(), 2);
    assertTrue(circles.contains(new ArrayList<>(
      Arrays.asList(LT, EQ, LTE))) ||
      circles.contains(new ArrayList<>(
        Arrays.asList(EQ, LTE, LT))) ||
      circles.contains(new ArrayList<>(
        Arrays.asList(LTE, LT, EQ)))
    );
    circles.contains(new ArrayList<>(Arrays.asList(EQ, EQ)));
  }

  @Test
  public void cycleTest2() {
    ArrayList<ComparisonExpressionTPGM> comps = new ArrayList<>(
      Arrays.asList(
        new ComparisonExpressionTPGM(
          new Comparison(sel1, LTE, literal1)),
        new ComparisonExpressionTPGM(
          new Comparison(literal1, EQ, sel3)),
        new ComparisonExpressionTPGM(
          new Comparison(sel3, EQ, literal2))
      )
    );
    RelationGraph graph = new RelationGraph(new HashSet<>(comps));
    List<List<Comparator>> cycles = graph.findAllCircles();
    assertEquals(cycles.size(), 2);
    assertEquals(cycles.get(0), new ArrayList<>(
      Arrays.asList(EQ, EQ)));
    assertEquals(cycles.get(1), new ArrayList<>(
      Arrays.asList(EQ, EQ)));
  }

  @Test
  public void cycleTest3() {
    ArrayList<ComparisonExpressionTPGM> comps = new ArrayList<>(
      Arrays.asList(
        new ComparisonExpressionTPGM(
          new Comparison(sel1, LT, literal1)),
        new ComparisonExpressionTPGM(
          new Comparison(literal1, LT, sel3)),
        new ComparisonExpressionTPGM(
          new Comparison(sel3, LTE, literal2))
      )
    );
    RelationGraph graph = new RelationGraph(new HashSet<>(comps));
    List<List<Comparator>> cycles = graph.findAllCircles();
    assertEquals(cycles.size(), 0);
  }

  @Test
  public void cycleTest4() {
    ArrayList<ComparisonExpressionTPGM> comps = new ArrayList<>(
      // 3 cycles:
      // sel1 < literal1 < sel3 < sel1
      // sel1 < literal1 < sel3 != sel2 < sel1
      // sel3 != sel2 != sel3
      Arrays.asList(
        new ComparisonExpressionTPGM(
          new Comparison(sel1, LT, literal1)),
        new ComparisonExpressionTPGM(
          new Comparison(literal1, LT, sel3)),
        new ComparisonExpressionTPGM(
          new Comparison(sel3, LT, sel1)),
        new ComparisonExpressionTPGM(
          new Comparison(sel3, NEQ, sel2)),
        new ComparisonExpressionTPGM(
          new Comparison(sel2, LT, sel1))
      )
    );
    RelationGraph graph = new RelationGraph(new HashSet<>(comps));
    List<List<Comparator>> cycles = graph.findAllCircles();
    assertEquals(cycles.size(), 3);
    assertTrue(cycles.contains(
      new ArrayList<>(Arrays.asList(LT, LT, LT))));
    assertTrue(cycles.contains(
      new ArrayList<>(Arrays.asList(LT, LT, LT, NEQ))) ||
      cycles.contains(
        new ArrayList<>(Arrays.asList(NEQ, LT, LT, LT))) ||
      cycles.contains(
        new ArrayList<>(Arrays.asList(LT, NEQ, LT, LT))) ||
      cycles.contains(
        new ArrayList<>(Arrays.asList(LT, LT, NEQ, LT)))
    );
    assertTrue(cycles.contains(
      new ArrayList<>(Arrays.asList(NEQ, NEQ))));
  }

}
