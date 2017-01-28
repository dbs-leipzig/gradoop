package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
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
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet());

    LeafNode fNode = new FilterAndProjectEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet());

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
      queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet());
    LeafNode fNode = new FilterAndProjectEdgesNode(null,
      "m", "f", "o",
      queryHandler.getPredicates().getSubCNF("f"), Sets.newHashSet());

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
      queryHandler.getPredicates().getSubCNF("e1"), Sets.newHashSet());
    LeafNode e2Node = new FilterAndProjectEdgesNode(null,
      "m", "e2", "o",
      queryHandler.getPredicates().getSubCNF("e2"), Sets.newHashSet());

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
}
