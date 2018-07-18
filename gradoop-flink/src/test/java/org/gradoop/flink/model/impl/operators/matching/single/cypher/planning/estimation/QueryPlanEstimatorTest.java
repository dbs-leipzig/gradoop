/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.CartesianProductNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class QueryPlanEstimatorTest extends EstimatorTestBase  {

  @Test
  public void testVertex() throws Exception {
    String query = "MATCH (n)-->(m:Person)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());

    QueryPlan queryPlan = new QueryPlan(nNode);
    QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
    assertThat(estimator.getCardinality(), is(11L));

    queryPlan = new QueryPlan(mNode);
    estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
    assertThat(estimator.getCardinality(), is(6L));
  }

  @Test
  public void testEdge() throws Exception {
    String query = "MATCH (n)-[e]->(m)-[f:knows]->(o)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    LeafNode fNode = new FilterAndProjectEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet(), false);

    QueryPlan queryPlan = new QueryPlan(eNode);
    QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
    assertThat(estimator.getCardinality(), is(24L));

    queryPlan = new QueryPlan(fNode);
    estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
    assertThat(estimator.getCardinality(), is(10L));
  }

  @Test
  public void testFixedPattern() throws Exception {
    String query = "MATCH (n)-[e]->(m)-[f]->(o)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode oNode = new FilterAndProjectVerticesNode(null, "o",
      queryHandler.getPredicates().getSubCNF("o"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);
    LeafNode fNode = new FilterAndProjectEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet(), false);

    // (n)-[e]->
    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    QueryPlan plan = new QueryPlan(neJoin);
    QueryPlanEstimator estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(24L));

    // (n)-[e]->(m)
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemJoin);
    estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(24L));

    // (n)-[e]->(m)-[f]->
    JoinEmbeddingsNode nemfJoin = new JoinEmbeddingsNode(nemJoin, fNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemfJoin);
    estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(72L));

    // (n)-[e]->(m)-[f]->(o)
    JoinEmbeddingsNode nemfoJoin = new JoinEmbeddingsNode(nemfJoin, oNode, Lists.newArrayList("o"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    plan = new QueryPlan(nemfoJoin);
    estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(72L));
  }

  @Test
  public void testVariablePattern() throws Exception {
    String query = "MATCH (n)-[e1:knows*1..2]->(m)-[e2]->(o)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode oNode = new FilterAndProjectVerticesNode(null, "o",
      queryHandler.getPredicates().getSubCNF("o"), Sets.newHashSet());
    LeafNode e1Node = new FilterAndProjectEdgesNode(null,
      "n", "e1", "m",
      queryHandler.getPredicates().getSubCNF("e1"), Sets.newHashSet(), false);
    LeafNode e2Node = new FilterAndProjectEdgesNode(null,
      "m", "e2", "o",
      queryHandler.getPredicates().getSubCNF("e2"), Sets.newHashSet(), false);

    ExpandEmbeddingsNode ne1Join = new ExpandEmbeddingsNode(nNode, e1Node,
      "n", "e", "m", 2, 2,
      ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode ne1mJoin = new JoinEmbeddingsNode(ne1Join, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode ne1me2Join = new JoinEmbeddingsNode(ne1mJoin, e2Node, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode ne1me2oJoin = new JoinEmbeddingsNode(ne1me2Join, oNode, Lists.newArrayList("o"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    QueryPlan queryPlan = new QueryPlan(ne1me2oJoin);

    QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(42L));
  }

  @Test
  public void testCartesianProductVertices() throws Exception {
    String query = "MATCH (a),(b)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode aNode = new FilterAndProjectVerticesNode(null, "a",
      queryHandler.getPredicates().getSubCNF("a"), Sets.newHashSet());
    LeafNode bNode = new FilterAndProjectVerticesNode(null, "b",
      queryHandler.getPredicates().getSubCNF("b"), Sets.newHashSet());
    CartesianProductNode crossNode = new CartesianProductNode(aNode, bNode, MatchStrategy
      .HOMOMORPHISM, MatchStrategy.ISOMORPHISM);

    QueryPlan queryPlan = new QueryPlan(crossNode);

    QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);

    assertThat(estimator.getCardinality(), is(STATS.getVertexCount() * STATS.getVertexCount()));
  }


  @Test
  public void testComplexCartesianProduct() throws Exception {
    String query = "MATCH (a:Person)-[e1:knows]->(b:Person),(c:Forum)-[e2:hasTag]->(d:Tag)";
    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode aNode = new FilterAndProjectVerticesNode(null, "a",
      queryHandler.getPredicates().getSubCNF("a"), Sets.newHashSet());
    LeafNode bNode = new FilterAndProjectVerticesNode(null, "b",
      queryHandler.getPredicates().getSubCNF("b"), Sets.newHashSet());
    LeafNode cNode = new FilterAndProjectVerticesNode(null, "c",
      queryHandler.getPredicates().getSubCNF("c"), Sets.newHashSet());
    LeafNode dNode = new FilterAndProjectVerticesNode(null, "d",
      queryHandler.getPredicates().getSubCNF("d"), Sets.newHashSet());

    LeafNode e1Node = new FilterAndProjectEdgesNode(null, "a","e1","b",
      queryHandler.getPredicates().getSubCNF("e1"), Sets.newHashSet(), false);
    LeafNode e2Node = new FilterAndProjectEdgesNode(null, "c","e2","d",
      queryHandler.getPredicates().getSubCNF("e2"), Sets.newHashSet(), false);

    BinaryNode ae1 = new JoinEmbeddingsNode(aNode, e1Node, Lists.newArrayList("a"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    BinaryNode ae1b = new JoinEmbeddingsNode(ae1, bNode, Lists.newArrayList("b"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    BinaryNode ce2 = new JoinEmbeddingsNode(cNode, e2Node, Lists.newArrayList("c"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
    BinaryNode ce2d = new JoinEmbeddingsNode(ce2, dNode, Lists.newArrayList("d"),
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    CartesianProductNode crossNode = new CartesianProductNode(ae1b, ce2d, MatchStrategy
      .HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

    QueryPlan ae1bPlan = new QueryPlan(ae1b);
    QueryPlan ce2dPlan = new QueryPlan(ce2d);
    QueryPlan crossPlan = new QueryPlan(crossNode);

    QueryPlanEstimator ae1bEstimator = new QueryPlanEstimator(ae1bPlan, queryHandler, STATS);
    QueryPlanEstimator ce2dEstimator = new QueryPlanEstimator(ce2dPlan, queryHandler, STATS);
    QueryPlanEstimator crossEstimator = new QueryPlanEstimator(crossPlan, queryHandler, STATS);

    assertThat(crossEstimator.getCardinality(), is(ae1bEstimator.getCardinality() *
      ce2dEstimator.getCardinality()));
  }

}
