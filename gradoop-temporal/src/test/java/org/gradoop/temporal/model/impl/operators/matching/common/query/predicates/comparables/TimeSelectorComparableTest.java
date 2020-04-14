package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.ElementSelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimeSelectorComparableTest {

    @Test
    public void testEvaluationReturnsLongValue() {
        //------------------------
        // test selector for each time field
        //------------------------
        TimeSelector
                selector1 = new TimeSelector("a", "tx_from");
        TimeSelectorComparable wrapper1 = new TimeSelectorComparable(selector1);

        TimeSelector
                selector2 = new TimeSelector("a", "tx_to");
        TimeSelectorComparable wrapper2 = new TimeSelectorComparable(selector2);

        TimeSelector
                selector3 = new TimeSelector("a", "val_from");
        TimeSelectorComparable wrapper3 = new TimeSelectorComparable(selector3);

        TimeSelector
                selector4 = new TimeSelector("a", "val_to");
        TimeSelectorComparable wrapper4 = new TimeSelectorComparable(selector4);

        //-----------------------------------------
        // evaluate on an embedding
        //-----------------------------------------
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        Long tx_from = 1234L;
        Long tx_to = 1234567L;
        Long valid_from = 987L;
        Long valid_to = 98765L;
        embedding.add(GradoopId.get(),new PropertyValue[]{}, tx_from, tx_to, valid_from, valid_to);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData.setTimeColumn("a", 0);

        assertEquals(PropertyValue.create(tx_from), wrapper1.evaluate(embedding, metaData));
        assertNotEquals(PropertyValue.create(String.valueOf(tx_from)),
                wrapper1.evaluate(embedding, metaData));

        assertEquals(PropertyValue.create(tx_to), wrapper2.evaluate(embedding, metaData));
        assertEquals(PropertyValue.create(valid_from), wrapper3.evaluate(embedding, metaData));
        assertEquals(PropertyValue.create(valid_to), wrapper4.evaluate(embedding, metaData));

        //----------------------------------------------------
        // evaluate on a GraphElement
        //----------------------------------------------------
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        vertex.setTransactionTime(new Tuple2<>(tx_from, tx_to));
        vertex.setValidTime(new Tuple2<>(valid_from, valid_to));

        assertEquals(PropertyValue.create(tx_from), wrapper1.evaluate(vertex));
        assertEquals(PropertyValue.create(tx_to), wrapper2.evaluate(vertex));
        assertEquals(PropertyValue.create(valid_from), wrapper3.evaluate(vertex));
        assertEquals(PropertyValue.create(valid_to), wrapper4.evaluate(vertex));
    }

    @Test(expected = NoSuchElementException.class)
    public void testThrowErrorIfElementNotPresent() {
        TimeSelector selector = new TimeSelector("a", "valid_from");
        TimeSelectorComparable wrapper = new TimeSelectorComparable(selector);

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(GradoopId.get(), new PropertyValue[]{},
                1234L, 12345L, 567L, 56789L);

        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
        metaData.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 0);
        metaData.setTimeColumn("b",0);

        wrapper.evaluate(embedding, metaData);
    }
}
