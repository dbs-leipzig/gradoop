package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Sets;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation.FilterEstimator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalEdgesNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectTemporalVerticesNode;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FilterEstimatorTest extends TemporalEstimatorTestBase {
    @Test
    public void testVertex() throws Exception {
        String query = "MATCH (n) WHERE n.tx_to.after(2019-01-01)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
                "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(11L));
        assertThat(elementEstimator.getSelectivity(), is(1d));
    }

    @Test
    public void testVertexWithLabel() throws Exception {
        String query = "MATCH (n:Tag)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        FilterAndProjectTemporalVerticesNode node = new FilterAndProjectTemporalVerticesNode(null,
                "n", queryHandler.getPredicates().getSubCNF("n"), Sets.newHashSet());

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(3L));
        assertThat(elementEstimator.getSelectivity(), is(1d));
    }

    @Test
    public void testEdge() throws Exception {
        String query = "MATCH (n)-[e]->(m) WHERE n.val_to.before(m.val_from)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(24L));
        assertThat(elementEstimator.getSelectivity(), is(1d));
    }

    @Test
    public void testEdgeWithLabel() throws Exception {
        String query = "MATCH (n)-[e:knows]->(m)";
        TemporalQueryHandler queryHandler = new TemporalQueryHandler(query);

        FilterAndProjectTemporalEdgesNode node = new FilterAndProjectTemporalEdgesNode(null,
                "n", "e", "m",
                queryHandler.getPredicates().getSubCNF("e"), Sets.newHashSet(), false);

        FilterEstimator elementEstimator = new FilterEstimator(queryHandler, STATS);
        elementEstimator.visit(node);

        assertThat(elementEstimator.getCardinality(), is(10L));
        assertThat(elementEstimator.getSelectivity(), is(1d));
    }
}
