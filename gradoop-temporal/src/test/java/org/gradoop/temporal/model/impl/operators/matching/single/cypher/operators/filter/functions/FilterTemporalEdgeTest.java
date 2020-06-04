package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTemporalEdgeTest {
    @Test
    public void testFilterTemporalEdge() throws QueryContradictoryException {
        String query = "MATCH (a)-[e]->(b) WHERE (e.val_to.before(2020-04-11)) AND e.prop=\"test\"";
        TemporalCNF cnf  = new TemporalQueryHandler(query).getCNF().getSubCNF("e");

        FilterTemporalEdge filter = new FilterTemporalEdge(cnf);

        TemporalEdgeFactory factory = new TemporalEdgeFactory();
        // e1 fulfills the predicate
        TemporalEdge e1 =
                factory.initEdge(GradoopId.get(), GradoopId.get(), GradoopId.get());
        e1.setProperty("prop", PropertyValue.create("test"));
        e1.setValidTo(1000L);

        //e2 does not fulfill the predicate
        TemporalEdge e2 =
                factory.initEdge(GradoopId.get(), GradoopId.get(), GradoopId.get());
        e2.setValidTo(0);

        try {
            assertTrue(filter.filter(e1));
            assertFalse(filter.filter(e2));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
