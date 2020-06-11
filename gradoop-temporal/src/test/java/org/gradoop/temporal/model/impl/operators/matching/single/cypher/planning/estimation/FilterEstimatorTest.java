package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Sets;
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

public class FilterEstimatorTest extends TemporalGradoopTestBase {

    TemporalGraphStatistics STATS;
    CNFEstimation EST;

    @Before
    public void setUp() throws Exception {
        STATS = new BinningTemporalGraphStatisticsFactory().fromGraph(
                loadCitibikeSample());
    }

    @Test
    public void testVertex() throws Exception {
        String query = "MATCH (n) WHERE n.tx_to.after(2013-07-20)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));
        EST = new CNFEstimation(STATS, queryHandler);

        FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
                "n", queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS, EST);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(30L));
        assertThat(elementEstimator.getSelectivity(), is(elementEstimator.getCnfEstimation()
                .estimateCNF(queryHandler.getCNF().getSubCNF("n"))));
    }

    @Test
    public void testVertexWithLabel() throws Exception {
        String query = "MATCH (n:Tag)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));
        EST = new CNFEstimation(STATS, queryHandler);

        FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
                "n", queryHandler.getCNF().getSubCNF("n"), Sets.newHashSet());

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS, EST);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(0L));
        assertEquals(elementEstimator.getSelectivity(), 0., 0.001);
    }

    @Test
    public void testEdge() throws Exception {
        String query = "MATCH (n)-[e]->(m) WHERE n.val_to.before(m.val_from)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));
        EST = new CNFEstimation(STATS, queryHandler);

        FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS, EST);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(20L));
        assertThat(elementEstimator.getSelectivity(), is(
                elementEstimator.getCnfEstimation().estimateCNF(
                queryHandler.getCNF().getSubCNF("e")
        )));
    }

    @Test
    public void testEdgeWithLabel() throws Exception {
        String query = "MATCH (n)-[e:unknown]->(m)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query, 
                new CNFPostProcessing(new ArrayList<>()));
        EST = new CNFEstimation(STATS, queryHandler);

        FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getCNF().getSubCNF("e"), Sets.newHashSet(), false);

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS, EST);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(0L));
        assertEquals(elementEstimator.getSelectivity(), 0., 0.001);
    }
}
