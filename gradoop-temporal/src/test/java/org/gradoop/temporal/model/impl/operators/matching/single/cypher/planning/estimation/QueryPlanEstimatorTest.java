package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.CartesianProductNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsTPGMNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinTPGMEmbeddingsNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class QueryPlanEstimatorTest extends TemporalGradoopTestBase {

    TemporalGraphStatistics STATS;

    @Before
    public void setUp() throws Exception {
        STATS = new BinningTemporalGraphStatisticsFactory().fromGraph(
                loadCitibikeSample());
    }

    @Test
    public void testVertex() throws Exception {
        String query = "MATCH (n)-->(m:station) WHERE n.tx_from.before(m.tx_to)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());

        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());

        QueryPlan queryPlan = new QueryPlan(nNode);
        // 30 stations
        QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
        assertThat(estimator.getCardinality(), is(30L));

        queryPlan = new QueryPlan(mNode);
        estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
        // 30 stations
        assertThat(estimator.getCardinality(), is(30L));
    }

    @Test
    public void testEdge() throws Exception {
        String query = "MATCH (n)-[e]->(m)-[f:trip]->(o) WHERE n.tx.overlaps(o.val)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        LeafNode fNode = new FilterAndProjectTemporalEdgesNode(null,
                "m", "f", "o",
                queryHandler.getCNF().getSubCNF("f"), Sets.newHashSet(), false);

        QueryPlan queryPlan = new QueryPlan(eNode);
        QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
        // 20 edges
        assertThat(estimator.getCardinality(), is(20L));

        queryPlan = new QueryPlan(fNode);
        estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);
        assertThat(estimator.getCardinality(), is(20L));
    }

    @Test
    public void testFixedPattern() throws Exception {
        // 2 matches for ISO
        String query = "MATCH (n)-[e]->(m)-[f]->(o) WHERE n.tx_to>1970-01-01";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode oNode = new FilterAndProjectTemporalVerticesNode(null, "o",
                queryHandler.getCNF().getSubCNF("o"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);
        LeafNode fNode = new FilterAndProjectTemporalEdgesNode(null,
                "m", "f", "o",
                queryHandler.getCNF().getSubCNF("f"), Sets.newHashSet(), false);

        QueryPlan plan = new QueryPlan(nNode);
        QueryPlanEstimator estimator = new QueryPlanEstimator(plan, queryHandler, STATS);
        System.out.println(estimator.getCardinality());

        // (n)-[e]->
        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(neJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assertThat(estimator.getCardinality(), is(20L));

        // (n)-[e]->(m)
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assertThat(estimator.getCardinality(), is(20L));

        // (n)-[e]->(m)-[f]->
        JoinTPGMEmbeddingsNode nemfJoin = new JoinTPGMEmbeddingsNode(nemJoin, fNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemfJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        // 20*30*20*30 / (30*17*30)  rather inaccurate,...
        assertThat(estimator.getCardinality(), is(24L));

        // (n)-[e]->(m)-[f]->(o)
        JoinTPGMEmbeddingsNode nemfoJoin = new JoinTPGMEmbeddingsNode(nemfJoin, oNode, Lists.newArrayList("o"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemfoJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assertThat(estimator.getCardinality(), is(24L));
    }


    @Test
    public void testCartesianProductVertices() throws Exception {
        // prohibit asOf(now)
        String query = "MATCH (a) (b) WHERE a.tx_to>1970-01-01";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
                queryHandler.getCNF().getSubCNF("a"), Sets.newHashSet());
        LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
                queryHandler.getCNF().getSubCNF("b"), Sets.newHashSet());
        CartesianProductNode crossNode = new CartesianProductNode(aNode, bNode, MatchStrategy
                .HOMOMORPHISM, MatchStrategy.ISOMORPHISM);

        QueryPlan queryPlan = new QueryPlan(crossNode);

        QueryPlanEstimator estimator = new QueryPlanEstimator(queryPlan, queryHandler, STATS);

        assertThat(estimator.getCardinality(), is(STATS.getVertexCount() * STATS.getVertexCount()));
    }


    @Test
    public void testComplexCartesianProduct() throws Exception {
        String query = "MATCH (a:Person)-[e1:knows]->(b:Person),(c:Forum)-[e2:hasTag]->(d:Tag)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
                queryHandler.getCNF().getSubCNF("a"), Sets.newHashSet());
        LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
                queryHandler.getCNF().getSubCNF("b"), Sets.newHashSet());
        LeafNode cNode = new FilterAndProjectTemporalVerticesNode(null, "c",
                queryHandler.getCNF().getSubCNF("c"), Sets.newHashSet());
        LeafNode dNode = new FilterAndProjectTemporalVerticesNode(null, "d",
                queryHandler.getCNF().getSubCNF("d"), Sets.newHashSet());

        LeafNode e1Node = new FilterAndProjectTemporalEdgesNode(null, "a", "e1", "b",
                queryHandler.getCNF().getSubCNF("e1"), Sets.newHashSet(), false);
        LeafNode e2Node = new FilterAndProjectTemporalEdgesNode(null, "c", "e2", "d",
                queryHandler.getCNF().getSubCNF("e2"), Sets.newHashSet(), false);

        BinaryNode ae1 = new JoinTPGMEmbeddingsNode(aNode, e1Node, Lists.newArrayList("a"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        BinaryNode ae1b = new JoinTPGMEmbeddingsNode(ae1, bNode, Lists.newArrayList("b"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        BinaryNode ce2 = new JoinTPGMEmbeddingsNode(cNode, e2Node, Lists.newArrayList("c"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        BinaryNode ce2d = new JoinTPGMEmbeddingsNode(ce2, dNode, Lists.newArrayList("d"),
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
