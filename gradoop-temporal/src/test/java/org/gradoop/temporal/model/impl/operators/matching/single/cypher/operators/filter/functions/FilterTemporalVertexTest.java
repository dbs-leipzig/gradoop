package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTemporalVertexTest {

    @Test
    public void testFilterTemporalVertex() throws QueryContradictoryException {
        String query = "MATCH (a) WHERE (1970-01-01.before(a.tx_from)) AND a.prop=\"test\"";
        TemporalCNF cnf  = new TemporalQueryHandler(query).getCNF();

        FilterTemporalVertex filter = new FilterTemporalVertex(cnf);

        TemporalVertexFactory factory = new TemporalVertexFactory();
        // v1 fulfills the predicate
        TemporalVertex v1 = factory.initVertex(GradoopId.get());
        v1.setProperty("prop", "test");
        v1.setTxFrom(1000);

        //v2 does not fulfill the predicate
        TemporalVertex v2 = factory.initVertex(GradoopId.get());
        v2.setProperty("prop", "test");
        v2.setTxFrom(0);

        try {
            assertTrue(filter.filter(v1));
            assertFalse(filter.filter(v2));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }
}
