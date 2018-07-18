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
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class JoinEstimatorTest extends EstimatorTestBase {

  @Test
  public void testLabelFree() throws Exception {
    String query = "MATCH (n)-[e]->(m)";

    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    estimator.visit(nemJoin);

    assertThat(estimator.getCardinality(), is(24L));
  }

  @Test
  public void testWithVertexLabels() throws Exception {
    String query = "MATCH (n:Forum)-[e]->(m:Tag)";

    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    estimator.visit(nemJoin);

    assertThat(estimator.getCardinality(), is(3L));
  }

  @Test
  public void testWithEdgeLabels() throws Exception {
    String query = "MATCH (n)-[e:knows]->(m)";

    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    estimator.visit(nemJoin);

    assertThat(estimator.getCardinality(), is(10L));
  }

  @Test
  public void testWithLabels() throws Exception {
    String query = "MATCH (n:Person)-[e:knows]->(m:Person)";

    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    assertThat(estimator.getCardinality(), is(10L));
    estimator.visit(nemJoin);
    assertThat(estimator.getCardinality(), is(10L));
  }

  @Test
  public void testWithLabelsUnbound() throws Exception {
    String query = "MATCH (:Person)-[:knows]->(:Person)";

    QueryHandler queryHandler = new QueryHandler(query);

    LeafNode nNode = new FilterAndProjectVerticesNode(null, "__v0",
      queryHandler.getPredicates().getSubCNF("__v0"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "__v1",
      queryHandler.getPredicates().getSubCNF("__v1"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "__v0", "__e0", "__v1",
      queryHandler.getPredicates().getSubCNF("__e0"), Sets.newHashSet(), false);

    JoinEmbeddingsNode neJoin = new JoinEmbeddingsNode(nNode, eNode, Lists.newArrayList("__v0"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("__v1"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    assertThat(estimator.getCardinality(), is(10L));
    estimator.visit(nemJoin);
    assertThat(estimator.getCardinality(), is(10L));
  }

  @Test
  public void testPathVariableLength() throws Exception {
    String query = "MATCH (n)-[e*1..2]->(m)";

    QueryHandler queryHandler = new QueryHandler(query);
    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    ExpandEmbeddingsNode neJoin = new ExpandEmbeddingsNode(nNode, eNode,
      "n", "e", "m", 1, 10,
      ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    estimator.visit(nemJoin);
    // 24 1-edge paths + 10 2-edge paths
    assertThat(estimator.getCardinality(), is(34L));
  }

  @Test
  public void testPathFixedLength() throws Exception {
    String query = "MATCH (n)-[e*2..2]->(m)";

    QueryHandler queryHandler = new QueryHandler(query);
    LeafNode nNode = new FilterAndProjectVerticesNode(null, "n",
      queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());
    LeafNode mNode = new FilterAndProjectVerticesNode(null, "m",
      queryHandler.getPredicates().getSubCNF("m"), Sets.newHashSet());
    LeafNode eNode = new FilterAndProjectEdgesNode(null,
      "n", "e", "m",
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

    ExpandEmbeddingsNode neJoin = new ExpandEmbeddingsNode(nNode, eNode,
      "n", "e", "m", 1, 10,
      ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
    JoinEmbeddingsNode nemJoin = new JoinEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(neJoin);
    estimator.visit(nemJoin);

    assertThat(estimator.getCardinality(), is(10L));
  }

  @Test
  public void testEmbeddedPathFixedLength() throws Exception {
    String query = "MATCH (n)-[e1*2..2]->(m)-[e2]->(o)";

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

    JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
    estimator.visit(ne1me2oJoin);
    estimator.visit(ne1me2Join);
    estimator.visit(ne1mJoin);
    estimator.visit(ne1Join);

    assertThat(estimator.getCardinality(), is(30L));
  }
}
