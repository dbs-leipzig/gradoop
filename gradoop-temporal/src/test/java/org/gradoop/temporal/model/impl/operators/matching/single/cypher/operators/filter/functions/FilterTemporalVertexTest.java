package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.AndPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import static org.junit.Assert.*;

public class FilterTemporalVertexTest {

    @Test
    public void testFilterTemporalVertex(){
        String query = "MATCH (a) WHERE (1970-01-01.before(a.tx_from)) AND a.prop=\"test\"";
        CNF cnf  = new TemporalQueryHandler(query).getPredicates();

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
