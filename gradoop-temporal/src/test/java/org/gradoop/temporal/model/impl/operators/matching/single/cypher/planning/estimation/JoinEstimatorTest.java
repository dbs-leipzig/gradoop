package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsTPGMNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinTPGMEmbeddingsNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class JoinEstimatorTest extends TemporalGradoopTestBase {

    TemporalGraphStatistics STATS;

    @Before
    public void setUp() throws Exception {
        STATS = new BinningTemporalGraphStatisticsFactory().fromGraph(
                loadCitibikeSample());
    }

    @Test
    public void testLabelFree() throws Exception {
        String query = "MATCH (n)-[e]->(m) WHERE n.tx_from.before(m.tx_from)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        estimator.visit(nemJoin);

        // there are 20 "trips" in the citibike sample graph, condition
        // always holds
        assertThat(estimator.getCardinality(), is(20L));
    }

    @Test
    public void testWithVertexLabels() throws Exception {
        // such nodes do not exist -> should be estimated 0
        String query = "MATCH (n:Forum)-[e]->(m:Tag) WHERE m.tx.overlaps(e.val)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        estimator.visit(nemJoin);

        assertThat(estimator.getCardinality(), is(0L));
    }

    @Test
    public void testWithEdgeLabels() throws Exception {
        // all edges have that label
        String query = "MATCH (n)-[e:trip]->(m) WHERE n.tx_to.before(m.tx_from)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        estimator.visit(nemJoin);

        assertThat(estimator.getCardinality(), is(20L));
    }

    @Test
    public void testWithLabels() throws Exception {
        String query = "MATCH (n:station)-[e:trip]->(m:station)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("n"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        assertThat(estimator.getCardinality(), is(20L));
        estimator.visit(nemJoin);
        assertThat(estimator.getCardinality(), is(20L));
    }

    @Test
    public void testWithLabelsUnbound() throws Exception {
        String query = "MATCH (:station)-[:trip]->(:station)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "__v0",
                queryHandler.getCNF().getSubCNF("__v0"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "__v1",
                queryHandler.getCNF().getSubCNF("__v1"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "__v0", "__e0", "__v1",
                queryHandler.getCNF().getSubCNF("__e0"), Sets.newHashSet(), false);

        JoinTPGMEmbeddingsNode neJoin = new JoinTPGMEmbeddingsNode(nNode, eNode, Lists.newArrayList("__v0"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("__v1"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        assertThat(estimator.getCardinality(), is(20L));
        estimator.visit(nemJoin);
        assertThat(estimator.getCardinality(), is(20L));
    }

    @Test
    public void testPathVariableLength() throws Exception {
        ExpansionCriteria noCriteria = new ExpansionCriteria();
        // 20+2 such paths (condition always holds)
        String query = "MATCH (n)-[e*1..2]->(m) WHERE n.tx.overlaps(m.val)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        ExpandEmbeddingsTPGMNode neJoin = new ExpandEmbeddingsTPGMNode(nNode, eNode,
                "n", "e", "m", 1, 10,
                ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM, noCriteria);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        estimator.visit(nemJoin);
        // 24 1-edge paths + 10 2-edge paths
        assertThat(estimator.getCardinality(), is(22L));
    }

    @Test
    public void testPathFixedLength() throws Exception {
        ExpansionCriteria noCriteria = new ExpansionCriteria();
        // 2 such paths (ISO!!!)
        String query = "MATCH (n)-[e*2..2]->(m)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode eNode = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        ExpandEmbeddingsTPGMNode neJoin = new ExpandEmbeddingsTPGMNode(nNode, eNode,
                "n", "e", "m", 1, 10,
                ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
                noCriteria);
        JoinTPGMEmbeddingsNode nemJoin = new JoinTPGMEmbeddingsNode(neJoin, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(neJoin);
        estimator.visit(nemJoin);

        assertThat(estimator.getCardinality(), is(2L));
    }

    @Test
    public void testEmbeddedPathFixedLength() throws Exception {
        ExpansionCriteria noCriteria = new ExpansionCriteria();
        // same as before
        String query = "MATCH (n)-[e1*2..2]->(m)-[e2]->(o) WHERE n.tx_from.before(o.tx_from)";

        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);
        LeafNode nNode = new FilterAndProjectTemporalVerticesNode(null, "n",
                queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());
        LeafNode mNode = new FilterAndProjectTemporalVerticesNode(null, "m",
                queryHandler.getCNF().getSubCNF("m"), Sets.newHashSet());
        LeafNode oNode = new FilterAndProjectTemporalVerticesNode(null, "o",
                queryHandler.getCNF().getSubCNF("o"), Sets.newHashSet());
        LeafNode e1Node = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e1", "m",
                queryHandler.getCNF().getSubCNF("e1"), Sets.newHashSet(), false);
        LeafNode e2Node = new FilterAndProjectTemporalEdgesNode(null,
                "m", "e2", "o",
                queryHandler.getCNF().getSubCNF("e2"), Sets.newHashSet(), false);

        ExpandEmbeddingsTPGMNode ne1Join = new ExpandEmbeddingsTPGMNode(nNode, e1Node,
                "n", "e", "m", 2, 2,
                ExpandDirection.OUT, MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
                noCriteria);
        JoinTPGMEmbeddingsNode ne1mJoin = new JoinTPGMEmbeddingsNode(ne1Join, mNode, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode ne1me2Join = new JoinTPGMEmbeddingsNode(ne1mJoin, e2Node, Lists.newArrayList("m"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);
        JoinTPGMEmbeddingsNode ne1me2oJoin = new JoinTPGMEmbeddingsNode(ne1me2Join, oNode, Lists.newArrayList("o"),
                MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

        JoinEstimator estimator = new JoinEstimator(queryHandler, STATS);
        estimator.visit(ne1me2oJoin);
        estimator.visit(ne1me2Join);
        estimator.visit(ne1mJoin);
        estimator.visit(ne1Join);

        assertThat(estimator.getCardinality(), is(2L));
    }
}
