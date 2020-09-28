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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.TimeConstant;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

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

public class CNFEstimationTest extends TemporalGradoopTestBase {

  final TimeSelector aTxFrom = new TimeSelector("a", TX_FROM);
  final TimeSelector aTxTo = new TimeSelector("a", TX_TO);
  final TimeSelector aValFrom = new TimeSelector("a", VAL_FROM);
  final TimeSelector aValTo = new TimeSelector("a", VAL_TO);

  final TimeSelector bTxFrom = new TimeSelector("b", TX_FROM);
  final TimeSelector bTxTo = new TimeSelector("b", TX_TO);
  final TimeSelector cTxTo = new TimeSelector("c", TX_TO);
  final Duration eValDuration = new Duration(new TimeSelector("e", VAL_FROM),
    new TimeSelector("e", VAL_TO));
  final Duration eTxDuration = new Duration(new TimeSelector("e", TX_FROM),
    new TimeSelector("e", TX_TO));
  final Duration fValDuration = new Duration(new TimeSelector("f", VAL_FROM),
    new TimeSelector("f", VAL_TO));
  TimeSelector bValFrom = new TimeSelector("b", VAL_FROM);
  TimeSelector bValTo = new TimeSelector("b", VAL_TO);
  TimeSelector cTxFrom = new TimeSelector("c", TX_FROM);
  TimeSelector cValFrom = new TimeSelector("c", VAL_FROM);
  TimeSelector cValTo = new TimeSelector("c", VAL_TO);
  Duration cTxDuration = new Duration(new TimeSelector("c", TX_FROM),
    new TimeSelector("c", TX_TO));

  @Test
  public void timeSelectorComparisonTest() throws Exception {
    CNFEstimation estimator = getEstimator();

    //tx_from of v1 equally distributed from 100L to 200L
    // => 175L is part of the 76th bin
    // => 24 bins are greater => should yield 0.24
    Comparison comp1 = new Comparison(aTxFrom, GT, new TimeLiteral(175L));
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.24, 0.01);
    // does the cache work?
    double estimation1Cached = estimator.estimateCNF(cnf1);
    assertEquals(estimation1Cached, estimation1, .0);

    // switch sides
    Comparison comp2 = new Comparison(new TimeLiteral(175L), LT, aTxFrom);
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation2, estimation1, 0.);

    // comparisons where lhs and rhs are both selectors are estimated to 1.
    // (no matter how much sense they make)
    Comparison comp3 = new Comparison(aTxTo, EQ, aTxFrom);
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 1., 0.00001);
  }

  @Test
  public void durationComparisonTest() throws Exception {
    CNFEstimation estimator = getEstimator();

    // durations are equally distributed from 0 to 100
    Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.9, 0.03);

    // switch sides
    Comparison comp2 = new Comparison(new TimeConstant(10), LT, eValDuration);
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation2, estimation1, 0.);

    // durations that do not compare tx_from / val_duration with a constant
    // are evaluated to, i.e. neglected in the estimation
    Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 1., 0.0001);

    Comparison comp4 = new Comparison(new Duration(
      new TimeSelector("e", TX_FROM),
      new TimeLiteral("2020-05-01")),
      LT, new TimeConstant(20L));
    CNFElement e4 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp4, new ComparableTPGMFactory())));
    CNF cnf4 = new CNF(Collections.singletonList(e4));
    double estimation4 = estimator.estimateCNF(cnf4);
    assertEquals(estimation4, 1., 0.0001);
  }

  @Test
  public void complexDurationComparisonTest() throws Exception {
    CNFEstimation estimator = getEstimator();

    Comparison comp1 = new Comparison(eTxDuration, LTE, fValDuration);
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.5, 0.02);

    Comparison comp2 = new Comparison(eTxDuration, EQ, fValDuration);
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation2, 0., 0.01);

    Comparison comp3 = new Comparison(eTxDuration, LT, fValDuration);
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 0.5, 0.02);
    assertTrue(estimation3 < estimation1);
  }

  @Test
  public void propertyComparisonTest() throws Exception {
    CNFEstimation estimator = getEstimator();

    // categorical
    Comparison comp1 = new Comparison(
      new PropertySelector("a", "catProp1"),
      EQ,
      new Literal("x")
    );
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.06, 0.001);
    // sides switched
    Comparison comp2 = new Comparison(
      new Literal("x"),
      EQ,
      new PropertySelector("a", "catProp1")
    );
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation2, 0.06, 0.001);

    // numerical
    Comparison comp3 = new Comparison(
      new PropertySelector("c", "numProp"),
      LTE,
      new Literal(50)
    );
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 0.15, 0.02);
    // switched sides
    Comparison comp4 = new Comparison(
      new Literal(50),
      GTE,
      new PropertySelector("c", "numProp")
    );
    CNFElement e4 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp4, new ComparableTPGMFactory())));
    CNF cnf4 = new CNF(Collections.singletonList(e4));
    double estimation4 = estimator.estimateCNF(cnf4);
    assertEquals(estimation4, estimation3, 0.);
  }

  @Test
  public void complexPropertyComparisonTest() throws Exception {
    // property selector vs property selector
    CNFEstimation estimator = getEstimator();

    // categorical
    Comparison comp1 = new Comparison(
      new PropertySelector("b", "gender"),
      EQ,
      new PropertySelector("b", "gender")
    );
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    // (2/3)² * (1/3)²
    assertEquals(estimation1, 0.55, 0.01);


    // numerical
    Comparison comp2 = new Comparison(
      new PropertySelector("a", "numProp"),
      GTE,
      new PropertySelector("b", "numProp")
    );
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    // occurrences of numProp * prob(a.numProb >= b.numProb)
    // = (1/2)*(1/10) * (1/2) = 0.025
    assertEquals(estimation2, 0.025, 0.01);
    // switch sides
    Comparison comp3 = new Comparison(
      new PropertySelector("b", "numProp"),
      LT,
      new PropertySelector("a", "numProp")
    );
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, estimation2, 0.01);
  }

  @Test
  public void complexLiteralComparisonTest() throws Exception {
    CNFEstimation estimator = getEstimator();

    // cf. BinningTemporalGraphStatisticsTest

    Comparison comp1 = new Comparison(aValFrom, GTE, bTxTo);
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    double estimation1 = estimator.estimateCNF(new CNF(Collections.singletonList(e1)));
    assertEquals(estimation1, 0.25, 0.025);

    Comparison comp2 = new Comparison(aValTo, LT, cTxTo);
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    double estimation2 = estimator.estimateCNF(new CNF(Collections.singletonList(e2)));
    assertEquals(estimation2, 0.375, 0.1);

    Comparison comp3 = new Comparison(cTxTo, LTE, bTxFrom);
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    double estimation3 = estimator.estimateCNF(new CNF(Collections.singletonList(e3)));
    assertEquals(estimation3, 0.25, 0.05);
  }

  @Test
  public void conjunctionTest() throws Exception {
    CNFEstimation estimator = getEstimator();
    // ~0.9
    Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
    CNFElement e1 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp1, new ComparableTPGMFactory())));
    // ~0.125
    Comparison comp2 = new Comparison(
      new PropertySelector("a", "numProp"),
      GTE,
      new PropertySelector("a", "numProp")
    );
    CNFElement e2 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp2, new ComparableTPGMFactory())));
    // 1.
    Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
    CNFElement e3 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp3, new ComparableTPGMFactory())));
    // ~0.15
    Comparison comp4 = new Comparison(
      new Literal(50),
      GTE,
      new PropertySelector("c", "numProp")
    );
    CNFElement e4 =
      new CNFElement(Collections.singletonList(new ComparisonExpression(comp4, new ComparableTPGMFactory())));

    //0.9*0.125*0.15
    CNF cnf1 = new CNF(Arrays.asList(e1, e2, e4));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.017, 0.002);

    CNF cnf2 = new CNF(Arrays.asList(e1, e2, e3, e4));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation1, 0.017, 0.002);

    //0.9 * 0.125
    CNF cnf3 = new CNF(Arrays.asList(e1, e2));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 0.1125, 0.01);
  }

  @Test
  public void disjunctionTest() throws Exception {
    CNFEstimation estimator = getEstimator();
    // ~0.9     (a)
    Comparison comp1 = new Comparison(eTxDuration, GT, new TimeConstant(10));
    ComparisonExpression c1 = new ComparisonExpression(comp1, new ComparableTPGMFactory());
    // ~0.125      (b)
    Comparison comp2 = new Comparison(
      new PropertySelector("a", "numProp"),
      GTE,
      new PropertySelector("a", "numProp")
    );
    ComparisonExpression c2 = new ComparisonExpression(comp2, new ComparableTPGMFactory());
    // 1.       (c)
    Comparison comp3 = new Comparison(eTxDuration, LT, eValDuration);
    ComparisonExpression c3 = new ComparisonExpression(comp3, new ComparableTPGMFactory());
    // ~0.15    (d)
    Comparison comp4 = new Comparison(
      new Literal(50),
      GTE,
      new PropertySelector("c", "numProp")
    );
    ComparisonExpression c4 = new ComparisonExpression(comp4, new ComparableTPGMFactory());

    // (a) OR (b) = P(a)+P(b)-P(a)*P(b) = 0.9+0.125 - 0.9*0.125 = 0.9125
    CNFElement e1 = new CNFElement(Arrays.asList(c1, c2));
    CNF cnf1 = new CNF(Collections.singletonList(e1));
    double estimation1 = estimator.estimateCNF(cnf1);
    assertEquals(estimation1, 0.9125, 0.02);

    // (a) OR (b) OR (d)
    // = P(a) + P(b) + P(d) - P(a)*P(b) - P(a)*P(d) - P(b)*P(d) + P(a)*P(b)*P(d)
    // = 0.9+0.125+0.15 - 0.9*0.125 - 0.9*0.15 - 0.125*0.15 + 0.9*0.125*0.15
    // = 0.925625
    CNFElement e2 = new CNFElement(Arrays.asList(c1, c2, c4));
    CNF cnf2 = new CNF(Collections.singletonList(e2));
    double estimation2 = estimator.estimateCNF(cnf2);
    assertEquals(estimation2, 0.925, 0.02);

    // (a) OR (b) OR (c) = 1
    CNFElement e3 = new CNFElement(Arrays.asList(c1, c2, c3));
    CNF cnf3 = new CNF(Collections.singletonList(e3));
    double estimation3 = estimator.estimateCNF(cnf3);
    assertEquals(estimation3, 1., 0.01);

    // conjunction of disjunctions
    // independence assumption, should thus be ~ 0.9125*0.925*1. = 0.844
    CNF cnf4 = new CNF(Arrays.asList(e2, e1, e3));
    double estimation4 = estimator.estimateCNF(cnf4);
    assertEquals(estimation4, 0.84, 0.03);

    // check reordering of cnf4: sequence should be e1, e2, e3
    CNFElement e2Reordered = new CNFElement(Arrays.asList(c1, c4, c2));
    CNFElement e3Reordered = new CNFElement(Arrays.asList(c3, c1, c2));
    CNF cnf4Reordered = new CNF(Arrays.asList(
      e1, e2Reordered, e3Reordered));
    assertEquals(estimator.reorderCNF(cnf4), cnf4Reordered);

  }

  /**
   * Creates a CNFEstimation over a test graph (see {@link this::getDummyStats}.
   * Variable to label mapping: "a"->"v1", "b"->"v2", e->"edge"
   * Variable to type mapping "a"->VERTEX, "b"->VERTEX, "c"->VERTEX, "e"->EDGE
   * ("c" has no label specified)
   *
   * @return CNFEstimation object
   */
  private CNFEstimation getEstimator() throws Exception {
    BinningTemporalGraphStatistics stats = getDummyStats();

    HashMap<String, String> labelMap = new HashMap<>();
    labelMap.put("a", "v1");
    labelMap.put("b", "v2");
    labelMap.put("e", "edge");

    HashMap<String, TemporalGraphStatistics.ElementType> typeMap = new HashMap<>();
    typeMap.put("a", TemporalGraphStatistics.ElementType.VERTEX);
    typeMap.put("b", TemporalGraphStatistics.ElementType.VERTEX);
    typeMap.put("c", TemporalGraphStatistics.ElementType.VERTEX);
    typeMap.put("e", TemporalGraphStatistics.ElementType.EDGE);

    return new CNFEstimation(stats, typeMap, labelMap);
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
   *
   * @return dummy statistics
   * @throws Exception if anything flink-related goes wrong
   */
  private BinningTemporalGraphStatistics getDummyStats() throws Exception {
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
    // lengths are equally distributed from 0 to 100
    for (int i = 0; i < numEdges; i++) {
      TemporalEdge edge = new TemporalEdge();
      edge.setId(GradoopId.get());
      edge.setLabel(edgeLabel);
      edge.setTransactionTime(new Tuple2<>((long) i, (long) i + i));
      edge.setValidTime(edge.getTransactionTime());
      edgeList.add(edge);
    }

    TemporalGraph graph = new TemporalGraphFactory(getConfig()).fromCollections(
      vertexList, edgeList
    );

    return new BinningTemporalGraphStatisticsFactory()
      .fromGraphWithSampling(graph, 100);
  }

}
