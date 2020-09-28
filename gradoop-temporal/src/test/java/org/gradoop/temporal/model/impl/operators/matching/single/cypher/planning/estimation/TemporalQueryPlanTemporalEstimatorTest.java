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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.CartesianProductNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TemporalQueryPlanTemporalEstimatorTest extends TemporalGradoopTestBase {

  TemporalGraphStatistics stats;
  CNFEstimation est;

  @Before
  public void setUp() throws Exception {
    stats = new BinningTemporalGraphStatisticsFactory().fromGraph(
      loadCitibikeSample());
  }

  @Test
  public void testVertex() throws Exception {
    String query = "MATCH (n)-->(m:station) WHERE n.tx_from.before(m.tx_to)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());

    QueryPlan queryPlan = new QueryPlan(nNode);
    // 30 stations
    TemporalQueryPlanEstimator estimator =
      new TemporalQueryPlanEstimator(queryPlan, queryHandler, stats, est);
    assertThat(estimator.getCardinality(), is(30L));

    queryPlan = new QueryPlan(mNode);
    estimator = new TemporalQueryPlanEstimator(queryPlan, queryHandler, stats, est);
    // 30 stations
    assertThat(estimator.getCardinality(), is(30L));
  }

  @Test
  public void testEdge() throws Exception {
    String query = "MATCH (n)-[e]->(m)-[f:trip]->(o) WHERE n.tx.overlaps(o.val)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    LeafNode fNode = new FilterAndProjectTemporalEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet(), false);


    QueryPlan queryPlan = new QueryPlan(eNode);
    TemporalQueryPlanEstimator estimator =
      new TemporalQueryPlanEstimator(queryPlan, queryHandler, stats, est);
    // 20 edges
    assertThat(estimator.getCardinality(), is(20L));

    queryPlan = new QueryPlan(fNode);
    estimator = new TemporalQueryPlanEstimator(queryPlan, queryHandler, stats, est);
    assertThat(estimator.getCardinality(), is(20L));
  }

  @Test
  public void testFixedPattern() throws Exception {
    // 2 matches for ISO
    String query = "MATCH (n)-[e]->(m)-[f]->(o) WHERE n.tx_to>Timestamp(1970-01-01)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode oNode = new FilterAndProjectTemporalVerticesNode(null, "o",
      queryHandler.getPredicates().getSubCNF("o"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);
    LeafNode fNode = new FilterAndProjectTemporalEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet(), false);

    QueryPlan plan = new QueryPlan(nNode);
    TemporalQueryPlanEstimator estimator = new TemporalQueryPlanEstimator(plan, queryHandler, stats, est);

    // (n)-[e]->
    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(neJoin);
    estimator = new TemporalQueryPlanEstimator(plan, queryHandler, stats, est);

    assertTrue(18 <= estimator.getCardinality() && estimator.getCardinality() <= 20);

    // (n)-[e]->(m)
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemJoin);
    estimator = new TemporalQueryPlanEstimator(plan, queryHandler, stats, est);

    assert 18 <= estimator.getCardinality() && estimator.getCardinality() <= 20;

    // (n)-[e]->(m)-[f]->
    JoinEmbeddingsNode nemfJoin = new JoinEmbeddingsNode(nemJoin, fNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemfJoin);
    estimator = new TemporalQueryPlanEstimator(plan, queryHandler, stats, est);

    // 20*30*20*30 / (30*17*30)  rather inaccurate,...
    assertTrue(22 <= estimator.getCardinality() && estimator.getCardinality() <= 24);

    // (n)-[e]->(m)-[f]->(o)
    JoinEmbeddingsNode nemfoJoin = new JoinEmbeddingsNode(nemfJoin, oNode, Lists.newArrayList("o"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemfoJoin);
    estimator = new TemporalQueryPlanEstimator(plan, queryHandler, stats, est);

    assertTrue(20 <= estimator.getCardinality() && estimator.getCardinality() <= 30);
  }


  @Test
  public void testCartesianProductVertices() throws Exception {
    // prohibit asOf(now)
    String query = "MATCH (a) (b) WHERE a.tx_to>Timestamp(1970-01-01)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
      queryHandler.getPredicates().getSubCNF("a"), Sets.newHashSet());
    LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
      queryHandler.getPredicates().getSubCNF("b"), Sets.newHashSet());
    CartesianProductNode crossNode = new CartesianProductNode(aNode, bNode, MatchStrategy
      .HOMOMORPHISM, MatchStrategy.ISOMORPHISM);

    QueryPlan queryPlan = new QueryPlan(crossNode);

    TemporalQueryPlanEstimator estimator =
      new TemporalQueryPlanEstimator(queryPlan, queryHandler, stats, est);

    assertTrue(850 <= estimator.getCardinality() && estimator.getCardinality() <= 900);
  }


  @Test
  public void testComplexCartesianProduct() throws Exception {
    String query = "MATCH (a)-[e1:trip]->(b:station),(c:station)-[e2:trip]->(d:station)" +
      "WHERE a.tx_from > c.val_from AND a.tx_to>Timestamp(1970-01-01)";
    TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
      new CNFPostProcessing(new ArrayList<>()));
    est = new CNFEstimation(stats, queryHandler);

    LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
      queryHandler.getPredicates().getSubCNF("a"), Sets.newHashSet());
    TemporalQueryPlanEstimator
      aEstimator = new TemporalQueryPlanEstimator(new QueryPlan(aNode), queryHandler, stats, est);
    assertTrue(28 <= aEstimator.getCardinality() && aEstimator.getCardinality() <= 30);

    LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
      queryHandler.getPredicates().getSubCNF("b"), Sets.newHashSet());
    TemporalQueryPlanEstimator
      bEstimator = new TemporalQueryPlanEstimator(new QueryPlan(bNode), queryHandler, stats, est);
    assertEquals(bEstimator.getCardinality(), 30);

    LeafNode cNode = new FilterAndProjectTemporalVerticesNode(null, "c",
      queryHandler.getPredicates().getSubCNF("c"), Sets.newHashSet());
    TemporalQueryPlanEstimator
      cEstimator = new TemporalQueryPlanEstimator(new QueryPlan(cNode), queryHandler, stats, est);
    assertEquals(cEstimator.getCardinality(), 30);

    LeafNode dNode = new FilterAndProjectTemporalVerticesNode(null, "d",
      queryHandler.getPredicates().getSubCNF("d"), Sets.newHashSet());
    TemporalQueryPlanEstimator
      dEstimator = new TemporalQueryPlanEstimator(new QueryPlan(dNode), queryHandler, stats, est);
    assertEquals(dEstimator.getCardinality(), 30);

    LeafNode e1Node = new FilterAndProjectTemporalEdgesNode(null, "a", "e1", "b",
      queryHandler.getPredicates().getSubCNF("e1"), Sets.newHashSet(), false);
    TemporalQueryPlanEstimator
      e1Estimator = new TemporalQueryPlanEstimator(new QueryPlan(e1Node), queryHandler, stats, est);
    assertTrue(18 <= e1Estimator.getCardinality() &&
      e1Estimator.getCardinality() <= 20);

    LeafNode e2Node = new FilterAndProjectTemporalEdgesNode(null, "c", "e2", "d",
      queryHandler.getPredicates().getSubCNF("e2"), Sets.newHashSet(), false);
    TemporalQueryPlanEstimator
      e2Estimator = new TemporalQueryPlanEstimator(new QueryPlan(e2Node), queryHandler, stats, est);
    assertTrue(18 <= e2Estimator.getCardinality() &&
      e2Estimator.getCardinality() <= 20);

    BinaryNode ae1 = new JoinEmbeddingsNode(aNode, e1Node, Lists.newArrayList("a"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    TemporalQueryPlanEstimator
      ae1Estimator = new TemporalQueryPlanEstimator(new QueryPlan(ae1), queryHandler, stats, est);
    assertTrue(18 <= ae1Estimator.getCardinality() &&
      ae1Estimator.getCardinality() <= 20);

    BinaryNode ae1b = new JoinEmbeddingsNode(ae1, bNode, Lists.newArrayList("b"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    TemporalQueryPlanEstimator
      ae1bEstimator = new TemporalQueryPlanEstimator(new QueryPlan(ae1b), queryHandler, stats, est);
    assertTrue(18 <= ae1bEstimator.getCardinality() &&
      ae1bEstimator.getCardinality() <= 20);

    BinaryNode ce2 = new JoinEmbeddingsNode(cNode, e2Node, Lists.newArrayList("c"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    BinaryNode ce2d = new JoinEmbeddingsNode(ce2, dNode, Lists.newArrayList("d"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    CartesianProductNode crossNode = new CartesianProductNode(ae1b, ce2d, MatchStrategy
      .HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    QueryPlan ce2dPlan = new QueryPlan(ce2d);
    TemporalQueryPlanEstimator
      ce2dEstimator = new TemporalQueryPlanEstimator(ce2dPlan, queryHandler, stats, est);
    assertTrue(18 <= ce2dEstimator.getCardinality() &&
      ce2dEstimator.getCardinality() <= 20);

    QueryPlan crossPlan = new QueryPlan(crossNode);
    TemporalQueryPlanEstimator
      crossEstimator = new TemporalQueryPlanEstimator(crossPlan, queryHandler, stats, est);

    // join predicate "a.tx_from > c.val_from" has selectivity ~50%
    long withoutPredicate = ce2dEstimator.getCardinality() * ae1bEstimator.getCardinality();
    assertTrue(0.45 * withoutPredicate <= crossEstimator.getCardinality() &&
      crossEstimator.getCardinality() <= 0.55 * withoutPredicate);
  }

}
