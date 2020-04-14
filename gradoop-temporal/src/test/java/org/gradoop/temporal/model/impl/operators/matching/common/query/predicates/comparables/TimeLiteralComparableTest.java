package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimeLiteralComparableTest {

    @Test
    public void testEvaluationReturnsTimeLiteral() {
        //-----------------------------------------------
        // test data
        //-----------------------------------------------
        String timeString1 = "1972-02-12T13:25:03";
        TimeLiteral literal = new TimeLiteral(timeString1);
        TimeLiteralComparable wrapper = new TimeLiteralComparable(literal);
        PropertyValue reference = PropertyValue.create(dateStringToLong(timeString1));

        //------------------------------------------------
        // test on embeddings
        //------------------------------------------------
        assertEquals(reference, wrapper.evaluate(null, null));
        assertNotEquals(PropertyValue.create(timeString1),
                wrapper.evaluate(null, null));
        //---------------------------------------------------
        // test on GraphElement
        //---------------------------------------------------
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        assertEquals(reference, wrapper.evaluate(vertex));
    }

    private Long dateStringToLong(String date){
        return LocalDateTime.parse(date).toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
    }
}
