package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        // (n)-[e]->
        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(neJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assertTrue(18 <= estimator.getCardinality() && estimator.getCardinality() <= 20);

        // (n)-[e]->(m)
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assert(18 <= estimator.getCardinality() && estimator.getCardinality() <= 20);

        // (n)-[e]->(m)-[f]->
        JoinTPGMEmbeddingsNode nemfJoin = new JoinTPGMEmbeddingsNode(nemJoin, fNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemfJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        // 20*30*20*30 / (30*17*30)  rather inaccurate,...
        assertTrue(22 <= estimator.getCardinality() && estimator.getCardinality() <= 24);

        // (n)-[e]->(m)-[f]->(o)
        JoinTPGMEmbeddingsNode nemfoJoin = new JoinTPGMEmbeddingsNode(nemfJoin, oNode, Lists.newArrayList("o"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        plan = new QueryPlan(nemfoJoin);
        estimator = new QueryPlanEstimator(plan, queryHandler, STATS);

        assertTrue(20 <= estimator.getCardinality() && estimator.getCardinality() <= 30);
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

        assertTrue(850 <= estimator.getCardinality() && estimator.getCardinality() <= 900);
    }


    @Test
    public void testComplexCartesianProduct() throws Exception {
        String query = "MATCH (a)-[e1:trip]->(b:station),(c:station)-[e2:trip]->(d:station)" +
                "WHERE a.tx_from > c.val_from AND a.tx_to>1970-01-01";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
                queryHandler.getCNF().getSubCNF("a"), Sets.newHashSet());
        QueryPlanEstimator aEstimator = new QueryPlanEstimator(new QueryPlan(aNode), queryHandler, STATS);
        assertTrue(28 <= aEstimator.getCardinality() && aEstimator.getCardinality() <= 30);

        LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
                queryHandler.getCNF().getSubCNF("b"), Sets.newHashSet());
        QueryPlanEstimator bEstimator = new QueryPlanEstimator(new QueryPlan(bNode), queryHandler, STATS);
        assertEquals(bEstimator.getCardinality(), 30);

        LeafNode cNode = new FilterAndProjectTemporalVerticesNode(null, "c",
                queryHandler.getCNF().getSubCNF("c"), Sets.newHashSet());
        QueryPlanEstimator cEstimator = new QueryPlanEstimator(new QueryPlan(cNode), queryHandler, STATS);
        assertEquals(cEstimator.getCardinality(), 30);

        LeafNode dNode = new FilterAndProjectTemporalVerticesNode(null, "d",
                queryHandler.getCNF().getSubCNF("d"), Sets.newHashSet());
        QueryPlanEstimator dEstimator = new QueryPlanEstimator(new QueryPlan(dNode), queryHandler, STATS);
        assertEquals(dEstimator.getCardinality(), 30);

        LeafNode e1Node = new FilterAndProjectTemporalEdgesNode(null, "a", "e1", "b",
                queryHandler.getCNF().getSubCNF("e1"), Sets.newHashSet(), false);
        QueryPlanEstimator e1Estimator = new QueryPlanEstimator(new QueryPlan(e1Node), queryHandler, STATS);
        assertTrue(18 <= e1Estimator.getCardinality() &&
                e1Estimator.getCardinality() <= 20);

        LeafNode e2Node = new FilterAndProjectTemporalEdgesNode(null, "c", "e2", "d",
                queryHandler.getCNF().getSubCNF("e2"), Sets.newHashSet(), false);
        QueryPlanEstimator e2Estimator = new QueryPlanEstimator(new QueryPlan(e2Node), queryHandler, STATS);
        assertTrue(18 <= e2Estimator.getCardinality() &&
                e2Estimator.getCardinality() <= 20);

        BinaryNode ae1 = new JoinTPGMEmbeddingsNode(aNode, e1Node, Lists.newArrayList("a"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        QueryPlanEstimator ae1Estimator = new QueryPlanEstimator(new QueryPlan(ae1), queryHandler, STATS);
        assertTrue(18 <= ae1Estimator.getCardinality() &&
                ae1Estimator.getCardinality() <= 20);

        BinaryNode ae1b = new JoinTPGMEmbeddingsNode(ae1, bNode, Lists.newArrayList("b"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        QueryPlanEstimator ae1bEstimator = new QueryPlanEstimator(new QueryPlan(ae1b), queryHandler, STATS);
        assertTrue(18 <= ae1bEstimator.getCardinality() &&
                ae1bEstimator.getCardinality() <= 20);

        BinaryNode ce2 = new JoinTPGMEmbeddingsNode(cNode, e2Node, Lists.newArrayList("c"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);
        BinaryNode ce2d = new JoinTPGMEmbeddingsNode(ce2, dNode, Lists.newArrayList("d"),
                MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

        CartesianProductNode crossNode = new CartesianProductNode(ae1b, ce2d, MatchStrategy
                .HOMOMORPHISM, MatchStrategy.HOMOMORPHISM);

        QueryPlan ce2dPlan = new QueryPlan(ce2d);
        QueryPlanEstimator ce2dEstimator = new QueryPlanEstimator(ce2dPlan, queryHandler, STATS);
        assertTrue(18 <= ce2dEstimator.getCardinality() &&
                ce2dEstimator.getCardinality() <= 20);

        QueryPlan crossPlan = new QueryPlan(crossNode);
        QueryPlanEstimator crossEstimator = new QueryPlanEstimator(crossPlan, queryHandler, STATS);

        // join predicate "a.tx_from > c.val_from" has selectivity ~50%
        long withoutPredicate = ce2dEstimator.getCardinality() * ae1bEstimator.getCardinality();
        assertTrue(0.45*withoutPredicate <= crossEstimator.getCardinality() &&
                crossEstimator.getCardinality() <= 0.55*withoutPredicate);
    }

    @Test
    public void testUnknownLabels() throws QueryContradictoryException {
        // unknown labels should yield to estimation near 0 (exactly 0 in this case,
        // as there are very few vertices in the DB)
        String query = "MATCH (a)-[e:trip]->(b:unknown)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query,
                new CNFPostProcessing(new ArrayList<>()));

        LeafNode aNode = new FilterAndProjectTemporalVerticesNode(null, "a",
                queryHandler.getCNF().getSubCNF("a"), Sets.newHashSet());
        assertTrue(new QueryPlanEstimator(new QueryPlan(aNode), queryHandler, STATS)
                .getCardinality() > 0);

        LeafNode bNode = new FilterAndProjectTemporalVerticesNode(null, "b",
                queryHandler.getCNF().getSubCNF("b"), Sets.newHashSet());
        assertEquals(new QueryPlanEstimator(new QueryPlan(bNode), queryHandler, STATS)
                .getCardinality(), 0);

        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null, "a","e",
                "b", queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);
        assertTrue(new QueryPlanEstimator(new QueryPlan(eNode), queryHandler, STATS)
                .getCardinality() > 0);

        CartesianProductNode crossNode1 = new CartesianProductNode(aNode, bNode, MatchStrategy
                .HOMOMORPHISM, MatchStrategy.ISOMORPHISM);
        assertEquals(new QueryPlanEstimator(new QueryPlan(crossNode1), queryHandler, STATS)
                .getCardinality(), 0);

        CartesianProductNode crossNode2 = new CartesianProductNode(crossNode1, eNode, MatchStrategy
                .HOMOMORPHISM, MatchStrategy.ISOMORPHISM);
        assertEquals(new QueryPlanEstimator(new QueryPlan(crossNode2), queryHandler, STATS)
                .getCardinality(), 0);
    }

}
