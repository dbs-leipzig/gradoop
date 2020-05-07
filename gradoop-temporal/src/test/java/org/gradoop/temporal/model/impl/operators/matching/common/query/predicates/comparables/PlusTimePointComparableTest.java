package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import scala.sys.Prop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PlusTimePointComparableTest {

    /*@Test
    public void testSimpleLiteralPlusTimePointComparable() {
        //-----------------------------------------------
        // test data
        //-----------------------------------------------
        String timeString1 = "1970-01-01T00:00:01";
        TimeLiteral literal = new TimeLiteral(timeString1);
        PlusTimePointComparable wrapper =
                new PlusTimePointComparable(new PlusTimePoint(literal, new TimeConstant(5)));

        PropertyValue reference = PropertyValue.create(1005L);

        //------------------------------------------------
        // test on embeddings
        //------------------------------------------------
        assertEquals(reference, wrapper.evaluate(null, null));
        //---------------------------------------------------
        // test on GraphElement
        //---------------------------------------------------
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        assertEquals(reference, wrapper.evaluate(vertex));
    }

    @Test
    public void testSimpleSelectorPlusTimePointComparable() {
        //-----------------------------------------------
        // test data
        //-----------------------------------------------
        TimeSelector selector = new TimeSelector("a", TimeSelector.TimeField.TX_FROM);
        PlusTimePointComparable wrapper =
                new PlusTimePointComparable(new PlusTimePoint(selector, new TimeConstant(10)));

        //------------------------------------------------
        // test on embeddings
        //------------------------------------------------
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        Long tx_from = 1000L;
        Long tx_to = 1234567L;
        Long valid_from = 987L;
        Long valid_to = 98765L;
        PropertyValue reference = PropertyValue.create(1010L);

        embedding.add(GradoopId.get(),new PropertyValue[]{}, tx_from, tx_to, valid_from, valid_to);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData.setTimeColumn("a", 0);

        assertEquals(wrapper.evaluate(embedding, metaData), reference);
        //---------------------------------------------------
        // test on GraphElement
        //---------------------------------------------------
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        vertex.setTxFrom(1000L);
        assertEquals(reference, wrapper.evaluate(vertex));
    }

    @Test
    public void testComplexPlusTimePointComparable(){
        // 2000 ms
        TimeLiteral literal = new TimeLiteral("1970-01-01T00:00:02");
        PlusTimePoint inner = new PlusTimePoint(literal, new TimeConstant(10L));
        PlusTimePoint outer = new PlusTimePoint(inner, new TimeConstant(1L));
        PlusTimePointComparable wrapper = new PlusTimePointComparable(outer);

        PropertyValue reference = PropertyValue.create(2011L);

        //------------------------------------------------
        // test on embeddings
        //------------------------------------------------
        assertEquals(reference, wrapper.evaluate(null, null));
        //---------------------------------------------------
        // test on GraphElement
        //---------------------------------------------------
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        assertEquals(reference, wrapper.evaluate(vertex));
    }*/
}
