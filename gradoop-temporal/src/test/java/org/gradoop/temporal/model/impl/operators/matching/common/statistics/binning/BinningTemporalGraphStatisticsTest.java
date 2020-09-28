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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics.ElementType.EDGE;
import static org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics.ElementType.VERTEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;
import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.GT;
import static org.s1ck.gdl.utils.Comparator.GTE;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

public class BinningTemporalGraphStatisticsTest extends TemporalGradoopTestBase {


  @Test
  public void simpleComparisonTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();
    //tx_from of v1 equally distributed from 100L to 200L
    // => 175L is part of the 76th bin
    // => 24 bins are greater => should yield 0.24
    double estimation1 = stats.estimateTemporalProb(VERTEX,
      Optional.of("v1"),
      TimeSelector.TimeField.TX_FROM, Comparator.GT, 175L);
    Assert.assertEquals(estimation1, 0.24, 0.01);

    // half of the tx_to values are bounded => should yield 0.5
    double estimation2 = stats.estimateTemporalProb(VERTEX,
      Optional.of("v2"), TX_TO, LT, Long.MAX_VALUE);
    Assert.assertEquals(estimation2, 0.5, 0.01);

    // should yield 0.99 (is in the 100th bin, 99 are smaller)
    double estimation3 = stats.estimateTemporalProb(VERTEX,
      Optional.of("v1"), VAL_TO, LTE, Long.MAX_VALUE);
    Assert.assertEquals(estimation3, .99, 0.01);

    double estimation4 = stats.estimateTemporalProb(VERTEX,
      Optional.of("v2"), VAL_FROM, GTE, 3011L);
    Assert.assertEquals(estimation4, .96, 0.01);

    double estimation5 = stats.estimateTemporalProb(VERTEX,
      Optional.of("v1"), TX_FROM, GTE, Long.MIN_VALUE);
    Assert.assertEquals(estimation5, 1., 0.01);
  }

  @Test
  public void comparisonWithoutLabelTest() throws Exception {
    // same comparisons as above, but without label specified
    BinningTemporalGraphStatistics stats = getDummyStats();
    // all v2 tx_froms are greater than 175L, thus the condition holds for about 5/8 of all vertices
    double estimation1 = stats.estimateTemporalProb(VERTEX,
      Optional.empty(),
      TimeSelector.TimeField.TX_FROM, Comparator.GT, 175L);
    Assert.assertEquals(estimation1, 0.615, 0.01);

    // half of all tx_to values are bounded => should yield 0.5
    double estimation2 = stats.estimateTemporalProb(VERTEX,
      Optional.empty(), TX_TO, LT, Long.MAX_VALUE);
    Assert.assertEquals(estimation2, 0.5, 0.01);

    // should yield 0.99 (is in the 100th of each bin, 99 are smaller)
    double estimation3 = stats.estimateTemporalProb(VERTEX,
      Optional.empty(), VAL_TO, LTE, Long.MAX_VALUE);
    Assert.assertEquals(estimation3, .99, 0.01);

    // 0.96 for v2 (see above), 0 for v1 (estimated 0.01) => expect 0.485
    double estimation4 = stats.estimateTemporalProb(VERTEX,
      Optional.empty(), VAL_FROM, GTE, 3011L);
    Assert.assertEquals(estimation4, .485, 0.01);

    double estimation5 = stats.estimateTemporalProb(VERTEX,
      Optional.empty(), TX_FROM, GTE, Long.MIN_VALUE);
    Assert.assertEquals(estimation5, 1., 0.01);
  }

  @Test
  public void durationComparisonTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();

    //works for unbounded (values hard to estimate, as they aren't actually
    // normally distributed here
    // all lengths normally distributed from 0 to 100
    double estimation1 = stats.estimateDurationProb(EDGE, Optional.empty(),
      EQ, false, 50L);
    assertEquals(estimation1, 0.01, 0.01);
    double estimation2 = stats.estimateDurationProb(EDGE, Optional.of("edge"),
      NEQ, true, 1000000L);
    assertEquals(estimation2, 1., 0.0001);
    double estimation3 = stats.estimateDurationProb(EDGE, Optional.empty(),
      LT, false, 75L);
    assertEquals(estimation3, 0.75, 0.1);
    double estimation4 = stats.estimateDurationProb(EDGE, Optional.empty(),
      LTE, true, 75L);
    assertEquals(estimation4, 0.75, 0.1);
    assertTrue(estimation4 > estimation3);
    double estimation5 = stats.estimateDurationProb(EDGE, Optional.of("edge"),
      GT, false, 50L);
    assertEquals(estimation5, 0.5, 0.05);
    double estimation6 = stats.estimateDurationProb(EDGE, Optional.of("edge"),
      GTE, true, 50L);
    assertEquals(estimation6, 0.5, 0.05);
    assertTrue(estimation6 > estimation5);
  }

  @Test
  public void complexDurationTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();
    //works for unbounded (values hard to estimate, as they aren't actually
    // normally distributed here
    // all lengths normally distributed from 0 to 100
    double estimation1 = stats.estimateDurationProb(EDGE, Optional.empty(),
      false, EQ, EDGE, Optional.of("edge"), true);
    assertEquals(estimation1, 0.01, 0.01);

    double estimation2 = stats.estimateDurationProb(EDGE, Optional.of("edge"), true,
      NEQ, EDGE, Optional.of("edge"), true);
    assertEquals(estimation2, 1., 0.01);

    double estimation3 = stats.estimateDurationProb(EDGE, Optional.empty(), false,
      LT, EDGE, Optional.empty(), false);
    assertEquals(estimation3, 0.5, 0.02);

    double estimation4 = stats.estimateDurationProb(EDGE, Optional.of("edge"),
      false, LTE, EDGE, Optional.empty(), false);
    assertTrue(estimation4 > estimation3);
    assertEquals(estimation4, 0.5, 0.2);

    double estimation5 = stats.estimateDurationProb(EDGE, Optional.of("edge"),
      false, GT, EDGE, Optional.of("edge"), true);
    assertEquals(estimation5, 0.5, 0.02);

    double estimation6 = stats.estimateDurationProb(EDGE, Optional.empty(), true,
      GTE, EDGE, Optional.of("edge"), false);
    assertTrue(estimation6 > estimation5);
    assertEquals(estimation6, 0.5, 0.02);

  }

  @Test
  public void estimateCategoricalTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();
    double estimation1 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"), "catProp1", EQ,
      PropertyValue.create("x"));
    assertEquals(estimation1, 0.06, 0.001);
    double estimation2 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"), "catProp1", EQ,
      PropertyValue.create("y"));
    assertEquals(estimation2, 0.34, 0.001);

    double estimation3 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"), "catProp1", NEQ,
      PropertyValue.create("y"));
    assertEquals(estimation3, 0.66, 0.001);

    double estimation4 = stats.estimatePropertyProb(VERTEX, Optional.empty(), "catProp1", EQ,
      PropertyValue.create("x"));
    assertEquals(estimation4, 0.03, 0.001);

    // 34 v1 nodes with property value y, 20 v2 nodes with property value y
    double estimation5 = stats.estimatePropertyProb(VERTEX, Optional.empty(), "catProp1", EQ,
      PropertyValue.create("y"));
    assertEquals(estimation5, 0.27, 0.001);
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.empty(), "catProp1", NEQ,
      PropertyValue.create("y"));
    assertEquals(estimation6, 0.73, 0.001);


    // unknown property and/or value
    double estimation7 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, PropertyValue.create("nonsense"));
    assertEquals(estimation7, 0.0001, 0.0001);
    double estimation8 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      NEQ, PropertyValue.create("nonsense"));
    assertEquals(estimation8, 0.9999, 0.0001);
    double estimation9 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "unknown",
      EQ, PropertyValue.create("nonsense"));
    assertEquals(estimation9, 0.0001, 0.0001);
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "unknown",
      NEQ, PropertyValue.create("nonsense"));
    assertEquals(estimation10, 0.0001, 0.0001);

  }

  @Test
  public void categoricalWithExclusionTest() throws Exception {
    // test with exclusion of properties
    BinningTemporalGraphStatistics stats = getDummyStats(new HashSet<>(), new HashSet<>());
    // should all be 0.5, regardless if actually there
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.empty(), "catProp1", EQ,
      PropertyValue.create("y"));
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "unknown",
      NEQ, PropertyValue.create("nonsense"));
    assertEquals(estimation6, 0.5, 0.00001);
    assertEquals(estimation10, 0.5, 0.0001);

    // with partial exclusion
    ArrayList<String> includeCategorical = new ArrayList<>(Collections.singletonList("catProp1"));
    BinningTemporalGraphStatistics stats2 = getDummyStats(new HashSet<>(),
      new HashSet<>(includeCategorical));
    estimation6 = stats2.estimatePropertyProb(VERTEX, Optional.empty(), "catProp1", EQ,
      PropertyValue.create("y"));
    assertEquals(estimation6, 0.27, 0.001);
    estimation10 = stats2.estimatePropertyProb(VERTEX, Optional.of("v2"), "unknown",
      NEQ, PropertyValue.create("nonsense"));
    assertEquals(estimation10, 0.5, 0.0001);
  }

  @Test
  public void estimateNumericalTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();
    // no exact estimations, as the values aren't actually normally distributed here
    // <=
    double estimation1 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", LTE, PropertyValue.create(50));
    assertTrue(0.2 <= estimation1 && estimation1 <= 0.3);
    double estimation2 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp", LTE, PropertyValue.create(50));
    assertTrue(0.12 <= estimation2 && estimation2 <= 0.18);

    // <
    double estimation3 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", LT, PropertyValue.create(50));
    assertTrue(0.2 <= estimation1 && estimation1 <= 0.3);
    assertTrue(estimation3 < estimation1);

    // =
    double estimation4 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", EQ, PropertyValue.create(50));
    assertEquals(0., estimation4, 0.001);

    // !=
    double estimation5 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"),
      "numProp", NEQ, PropertyValue.create(50));
    // occurs only in 1/10th of the nodes
    assertEquals(.1, estimation5, 0.01);

    // >
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", GT, PropertyValue.create(20));
    assertTrue(0.375 <= estimation6 && estimation6 <= 0.425);
    double estimation7 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", GT, PropertyValue.create(-5));
    assertTrue(estimation7 >= 0.475);
    double estimation8 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp", GT, PropertyValue.create(105));
    assertTrue(estimation8 <= 0.025);

    // >=
    double estimation9 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp", GTE, PropertyValue.create(105));
    assertTrue(estimation9 <= 0.025);
    assertTrue(estimation9 > estimation8);

    // unknown property -> estimation 0.0001
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "unknown", GTE, PropertyValue.create(20));
    assertEquals(estimation10, 0.0001, 0.001);
  }

  @Test
  public void numericalWithExclusion() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats(new HashSet<>(), new HashSet<>());
    // should all be 0.5
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", GT, PropertyValue.create(20));
    assertEquals(estimation6, 0.5, 0.0001);
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "unknown", GTE, PropertyValue.create(20));
    assertEquals(estimation10, 0.5, 0.001);

    // with partial exclusion
    ArrayList<String> includeNumerical = new ArrayList<>(Collections.singletonList("numProp"));
    BinningTemporalGraphStatistics stats2 = getDummyStats(new HashSet<>(includeNumerical),
      new HashSet<>());
    estimation6 = stats2.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp", GT, PropertyValue.create(20));
    assertTrue(0.375 <= estimation6 && estimation6 <= 0.425);
    estimation10 = stats2.estimatePropertyProb(VERTEX, Optional.empty(),
      "unknown", GTE, PropertyValue.create(20));
    assertEquals(estimation10, 0.5, 0.001);
  }

  @Test
  public void estimateComplexNumericalTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();

    // should yield about 1/8:
    // v1 occurs in every second vertex => 1/4 of all possible v1-v1-pairs
    // a comparison a>=b of two random numProp-values is expected to be true in 50% of the cases
    double estimation1 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"), "numProp",
      GTE, VERTEX, Optional.of("v1"), "numProp");
    assertEquals(estimation1, 0.125, 0.02);

    // should yield something between 0.066 and 0.1
    // 1/10 of all possible pairs match
    // numProp2 is equally distributed in [0,297], numProp1 in [0,99]
    double estimation2 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "numProp",
      LT, VERTEX, Optional.of("v1"), "numProp2");
    assertEquals(estimation2, 0.083, 0.017);

    // for estimation3 and estimation4:
    // matching pairs: 0.5*0.3 = 0.15 => P(numProp2>numProp) + P(numProp2<=numProp) = ~ 0.15
    // in most cases numProp2 > numProp
    double estimation3 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp",
      GT, VERTEX, Optional.empty(), "numProp2");
    assertEquals(estimation3, 0.025, 0.01);

    double estimation4 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp",
      LTE, VERTEX, Optional.empty(), "numProp2");
    assertEquals(estimation4, 0.125, 0.01);

    //doubling estimation4 by restricting numProp2 to v1 nodes
    //doubles the result, as numProp2 exclusively occurs in v1 nodes
    double estimation5 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp",
      LTE, VERTEX, Optional.of("v1"), "numProp2");
    assertEquals(estimation5, 0.25, 0.02);

    // EQ should yield a very small quantity, NEQ a very large
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp",
      EQ, VERTEX, Optional.empty(), "numProp");
    assertEquals(estimation6, 0., 0.01);
    // 1/20th of all pairs match => NEQ should be near to 0.05
    double estimation7 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp",
      NEQ, VERTEX, Optional.of("v2"), "numProp");
    assertEquals(estimation7, 0.05, 0.01);


    double estimation8 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "unknown",
      GTE, VERTEX, Optional.of("v2"), "test");
    assertEquals(estimation8, 0.0001, 0.001);
    double estimation9 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "unknown",
      LTE, VERTEX, Optional.of("v2"), "numProp");
    assertEquals(estimation9, 0.0001, 0.001);
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp",
      LT, VERTEX, Optional.of("v2"), "unknown");
    assertEquals(estimation10, 0.0001, 0.001);

  }

  @Test
  public void complexNumericalWithExclusion() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats(new HashSet<>(), new HashSet<>());
    double estimation4 = stats.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp",
      LTE, VERTEX, Optional.empty(), "numProp2");
    assertEquals(estimation4, 0.5, 0.01);
    double estimation8 = stats.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "unknown",
      GTE, VERTEX, Optional.of("v2"), "test");
    assertEquals(estimation8, 0.5, 0.001);

    // partial exclusion
    ArrayList<String> includeNumerical = new ArrayList<>(Collections.singletonList("numProp"));
    BinningTemporalGraphStatistics stats2 = getDummyStats(new HashSet<>(includeNumerical),
      new HashSet<>());
    estimation4 = stats2.estimatePropertyProb(VERTEX, Optional.empty(),
      "numProp",
      LTE, VERTEX, Optional.empty(), "numProp2");
    assertEquals(estimation4, 0.5, 0.01);
    estimation8 = stats2.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "unknown",
      GTE, VERTEX, Optional.of("v2"), "test");
    assertEquals(estimation8, 0.5, 0.001);
    double estimation7 = stats2.estimatePropertyProb(VERTEX, Optional.of("v1"),
      "numProp",
      NEQ, VERTEX, Optional.of("v2"), "numProp");
    assertEquals(estimation7, 0.05, 0.01);
  }

  @Test
  public void complexCategoricalTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();

    // 34% of the v2 nodes are set to m, rest to f
    // equal: 0.34²+0.66²= ~ 0.55
    double estimation1 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation1, 0.55, 0.01);

    double estimation2 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      NEQ, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation2, 0.45, 0.01);

    // 0 for comparators other than EQ and NEQ
    double estimation3 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      LT, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation3, 0., 0.001);
    double estimation4 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      LTE, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation4, 0., 0.001);
    double estimation5 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      GT, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation5, 0., 0.001);
    double estimation6 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      GTE, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation6, 0., 0.001);

    // property gender is only in v2
    double estimation7 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, VERTEX, Optional.of("v1"), "gender");
    assertEquals(estimation7, 0., 0.001);
    double estimation8 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      NEQ, VERTEX, Optional.of("v1"), "gender");
    assertEquals(estimation8, 0., 0.001);

    double estimation9 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, VERTEX, Optional.empty(), "gender");
    assertEquals(estimation9, 0.275, 0.001);
    double estimation10 = stats.estimatePropertyProb(VERTEX, Optional.empty(), "gender",
      EQ, VERTEX, Optional.empty(), "gender");
    assertEquals(estimation10, 0.1375, 0.001);

  }

  @Test
  public void complexCategoricalWithExclusion() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats(new HashSet<>(), new HashSet<>());
    double estimation2 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      NEQ, VERTEX, Optional.of("v2"), "gender");
    double estimation9 = stats.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, VERTEX, Optional.empty(), "gender");
    assertEquals(estimation9, 0.5, 0.001);
    assertEquals(estimation2, 0.5, 0.01);

    // with partial exclusion
    ArrayList<String> includeCategorical = new ArrayList<>(Collections.singletonList("gender"));
    BinningTemporalGraphStatistics stats2 = getDummyStats(new HashSet<>(),
      new HashSet<>(includeCategorical));
    estimation2 = stats2.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      NEQ, VERTEX, Optional.of("v2"), "gender");
    assertEquals(estimation2, 0.45, 0.01);
    double estimation1 = stats2.estimatePropertyProb(VERTEX, Optional.of("v2"), "gender",
      EQ, VERTEX, Optional.empty(), "catProp1");
    assertEquals(estimation1, 0.5, 0.01);
  }

  @Test
  public void complexTemporalTest() throws Exception {
    // estimations are quite inaccurate
    BinningTemporalGraphStatistics stats = getDummyStats();
    // about 0.25, as differences between bound values are small, but half of v2.tx_to unbound
    double estimation1 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_FROM,
      GTE, VERTEX, Optional.of("v2"), TX_TO);
    assertEquals(estimation1, 0.25, 0.025);

    // P(both unbound) = 0.25
    // P(both bound) = 0.25
    // P(lhs bound, rhs unbound) = 0.25
    // P(lhs unbound, rhs bound) = 0.25
    // P(lhs <= rhs | both bound) = 0.5
    // P(lhs <= rhs | both unbound) = 1.
    // P(lhs <= rhs | lhs bound, rhs unbound) = 1.
    // P(lhs <= rhs | lhs unbound, rhs bound) = 0
    // => P(lhs <= rhs) = 0.625
    double estimation2 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_TO,
      LTE, VERTEX, Optional.of("v1"), VAL_TO);
    assertEquals(estimation2, 0.625, 0.01);

    // nearly same as above, but P(lhs< rhs | both unbound) = 0
    // => P(lhs < rhs) = P(lhs <= rhs) - 0.25 = 0.375
    double estimation3 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_TO,
      LT, VERTEX, Optional.of("v1"), VAL_TO);
    assertTrue(estimation3 <= estimation2);
    assertEquals(estimation3, estimation2 - 0.25, 0.01);

    // should be very similar to estimation 1 (where <= instead of < was used)
    double estimation4 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_FROM,
      GT, VERTEX, Optional.of("v2"), TX_TO);
    assertTrue(estimation4 <= estimation1);
    assertEquals(estimation4, estimation1, 0.01);

    // for both types of vertices, half of the to-values are unbound
    // differences between bound values are neglectable, both types of vertices can be treated the same
    // P(both unbound) = P(both bound) = P(lhs bound, rhs not) = P(lhs unbound, rhs bound) = 0.25
    // P(lhs >= rhs | both bound) = 0.5
    // P(lhs >= rhs | both unbound) = 1.
    // P(lhs >= rhs | lhs, bound, rhs not) = 0.
    // P(lhs >= rhs | lhs unbound, rhs bound) = 1.
    // => P(lhs >= rhs) = 0.625
    double estimation5 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_TO,
      GTE, VERTEX, Optional.empty(), TX_TO);
    assertEquals(estimation5, 0.625, 0.02);

    // P(lhs <= rhs) > 0, only if both bound (P(both bound) = 0.5)
    // => P(lhs <= rhs) = 0.5^2 = 0.25
    double estimation6 = stats.estimateTemporalProb(VERTEX, Optional.empty(), TX_TO,
      LTE, VERTEX, Optional.of("v2"), TX_FROM);
    assertEquals(estimation6, 0.25, 0.01);

    // both bound => differences between bound values neglectable
    // P(lhs < rhs) = 0.5
    double estimation7 = stats.estimateTemporalProb(VERTEX, Optional.empty(), TX_FROM,
      LT, VERTEX, Optional.empty(), TX_FROM);
    assertEquals(estimation7, 0.5, 0.01);

    // all unbound => should be slightly below 0.5
    double estimation8 = stats.estimateTemporalProb(EDGE, Optional.empty(), TX_FROM,
      GT, VERTEX, Optional.of("v1"), VAL_FROM);
    assertEquals(estimation8, 0.5, 0.05);

    // only not the case, if both bound ((P(both bound) = 0.5). In this case, P(lhs <= rhs)=0.5
    // => P(lhs <= rhs) = 0.75
    double estimation9 = stats.estimateTemporalProb(EDGE, Optional.of("edge"), VAL_TO,
      LTE, VERTEX, Optional.empty(), VAL_TO);
    assertEquals(estimation9, 0.75, 0.01);

    // both identical, thus P(lhs <= rhs) = 0.5
    double estimation10 = stats.estimateTemporalProb(EDGE, Optional.of("edge"), VAL_TO,
      GTE, EDGE, Optional.empty(), TX_TO);
    assertEquals(estimation10, 0.5, 0.05);

    // EQ and NEQ
    // EQ should be very small here, as edges are never unbound
    double estimation11 = stats.estimateTemporalProb(VERTEX, Optional.of("v1"), VAL_TO,
      EQ, EDGE, Optional.of("edge"), TX_TO);
    assertEquals(estimation11, 0.001, 0.001);

    // NEQ should be very high, as all val-froms are never Long.MIN_VALUE
    double estimation12 = stats.estimateTemporalProb(VERTEX, Optional.empty(), VAL_FROM,
      NEQ, VERTEX, Optional.of("v1"), VAL_FROM);
    assertEquals(estimation12, 0.999, 0.01);

/*    // lhs=rhs, if both unbound (have value Long.MAX_VALUE)
    // P(both unbound) = 0.25
    double estimation13 = stats.estimateTemporalProb(VERTEX, Optional.empty(), VAL_TO,
      EQ, VERTEX, Optional.of("v1"), VAL_TO);
    assertEquals(estimation12, 0.25, 0.01);

    // inverse of above case
    double estimation14 = stats.estimateTemporalProb(VERTEX, Optional.empty(), VAL_TO,
      NEQ, VERTEX, Optional.of("v1"), VAL_TO);
    assertEquals(estimation12, 0.75, 0.01);*/

    // label not there
    double estimation13 = stats.estimateTemporalProb(VERTEX, Optional.empty(), VAL_FROM,
      LTE, VERTEX, Optional.of("unknownLabel"), VAL_FROM);
    assertEquals(estimation13, 0., 0.01);

    double estimation14 = stats.estimateTemporalProb(VERTEX, Optional.of("notThere"), TX_FROM,
      GT, EDGE, Optional.of("edge"), VAL_FROM);
    assertEquals(estimation14, 0., 0.01);
  }

  @Test
  public void countTest() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();
    long count1 = stats.getEdgeCount();
    assertEquals(count1, 100);
    long count2 = stats.getEdgeCount("edge");
    assertEquals(count2, 100);
    long count3 = stats.getEdgeCount("notThere");
    assertEquals(count3, 0);

    long count4 = stats.getVertexCount();
    assertEquals(count4, 200);
    long count5 = stats.getVertexCount("v1");
    assertEquals(count5, 100);
    long count6 = stats.getVertexCount("v2");
    assertEquals(count6, 100);
    long count7 = stats.getVertexCount("notThere");
    assertEquals(count7, 0);

    long count8 = stats.getDistinctSourceVertexCount();
    long count9 = stats.getDistinctSourceVertexCount("edge");
    assertEquals(count8, count9);
    assertEquals(count8, 10);
    long count10 = stats.getDistinctTargetVertexCount();
    long count11 = stats.getDistinctTargetVertexCount("edge");
    assertEquals(count10, count11);
    assertEquals(count11, 20);

    long count12 = stats.getDistinctSourceVertexCount("notThere");
    long count13 = stats.getDistinctTargetVertexCount("notThere");
    assertEquals(count12, count13);
    assertEquals(count12, 0);
  }

  /**
   * Creates a BinningTemporalGraphStatistics object
   * Vertices:
   * 100 "v1" vertices:
   * "catProp"   = "x" (6 vertices)
   * = "y" (34 vertices)
   * "numProp"   = 0,2,...,100 (50 vertices)
   * "numProp2"  = 0,3,6,...,297 (100 vertices)
   * tx_from goes from 100L to 200L, val_from from 150L to 250L (100 vertices)
   * tx_to goes from 300L to 350L for half of the vertices, other half is unbounded
   * val_to goes from 350L to 450L for half of the vertices, other half is unbounded
   * <p>
   * 100 "v2" vertices:
   * "catProp"   = "y" (20 vertices)
   * "numProp"   = 0,10,...,90 (10 vertices)
   * "gender"    = "m" (34 vertices)
   * = "f" (66 vertices)
   * tx_from goes from 1000L to 2000L, val_from from 3000L to 4000L (100 vertices)
   * tx_to goes from 1500L to 2500L (step 20) for half of the vertices, other half is unbounded
   * val_to goes from 3500L to 4500L for half of the vertices (step 20), other half is unbounded
   * <p>
   * Edges: identical tx and val times, their length equally distributed
   * from 0 to 100L
   * 10 different source vertices, 20 target vertices
   *
   * @return dummy statistics
   */
  private BinningTemporalGraphStatistics getDummyStats(Set<String> relevantNumerical,
                                                       Set<String> relevantCategorical) throws Exception {
    ArrayList<TemporalVertex> vertexList = new ArrayList<>();

    // first type of vertex has label1
    String vLabel1 = "v1";
    // tx_from goes from 100L to 200L, val_from from 150L to 250L (100 vertices)
    // tx_to goes from 300L to 350L for half of the vertices, other half is unbounded
    // val_to goes from 350L to 450L for half of the vertices, other half is unbounded
    int numL1Vertices = 100;
    for (int i = 0; i < numL1Vertices; i++) {
      TemporalVertex vertex = new TemporalVertex();
      vertex.setId(GradoopId.get());
      vertex.setLabel(vLabel1);

      vertex.setTxFrom(100L + i);
      Long txTo = i % 2 == 0 ? 300L + i : Long.MAX_VALUE;
      vertex.setTxTo(txTo);

      vertex.setValidFrom(150L + i);
      Long valTo = i % 2 == 0 ? 350L + i : Long.MAX_VALUE;
      vertex.setValidTo(valTo);

      // 6 nodes with property value x
      if (i % 10 == 0) {
        vertex.setProperty("catProp1", PropertyValue.create("x"));
      }
      // 34 nodes with property value y
      if (i % 3 == 0) {
        vertex.setProperty("catProp1", PropertyValue.create("y"));
      }

      // every second node has i as a property
      if (i % 2 == 0) {
        vertex.setProperty("numProp", PropertyValue.create(i));
      }

      vertex.setProperty("numProp2", PropertyValue.create(3 * i));

      vertexList.add(vertex);
    }

    // first type of vertex has label1
    String vLabel2 = "v2";
    // tx_from goes from 1000L to 2000L, val_from from 3000L to 4000L (100 vertices)
    // tx_to goes from 1500L to 2500L (step 20) for half of the vertices, other half is unbounded
    // val_to goes from 3500L to 4500L for half of the vertices (step 20), other half is unbounded
    int numL2Vertices = 100;
    for (int i = 0; i < numL2Vertices; i++) {
      TemporalVertex vertex = new TemporalVertex();
      vertex.setId(GradoopId.get());
      vertex.setLabel(vLabel2);

      vertex.setTxFrom(1000L + i * 10);
      Long txTo = i % 2 == 0 ? 1500L + i * 20 : Long.MAX_VALUE;
      vertex.setTxTo(txTo);

      vertex.setValidFrom(3000L + i * 10);
      Long valTo = i % 2 == 0 ? 3500L + i * 20 : Long.MAX_VALUE;
      vertex.setValidTo(valTo);

      if (i % 5 == 0) {
        vertex.setProperty("catProp1", "y");
      }

      // every 10th node has i as property
      if (i % 10 == 0) {
        vertex.setProperty("numProp", PropertyValue.create(i));
      }
      vertexList.add(vertex);

      if (i % 3 == 0) {
        vertex.setProperty("gender", PropertyValue.create("m"));
      } else {
        vertex.setProperty("gender", PropertyValue.create("f"));
      }
    }

    //edges (only one type)
    ArrayList<TemporalEdge> edgeList = new ArrayList<>();
    String edgeLabel = "edge";
    int numEdges = 100;
    // identical tx and val times.
    // All bounded, length equally distributed from 0 to 100
    for (int i = 0; i < numEdges; i++) {
      TemporalEdge edge = new TemporalEdge();
      edge.setId(GradoopId.get());
      edge.setLabel(edgeLabel);
      edge.setTransactionTime(new Tuple2<>((long) i, (long) i + i));
      edge.setValidTime(edge.getTransactionTime());
      edgeList.add(edge);
      edge.setSourceId(vertexList.get(i % 10).getId());
      edge.setTargetId(vertexList.get(i % 20).getId());
    }

    TemporalGraph graph = new TemporalGraphFactory(getConfig()).fromCollections(
      vertexList, edgeList
    );

    return new BinningTemporalGraphStatisticsFactory()
      .fromGraphWithSampling(graph, 100, relevantNumerical, relevantCategorical);

  }

  // without explicitly stating the categories
  private BinningTemporalGraphStatistics getDummyStats() throws Exception {
    return getDummyStats(null, null);
  }
}
