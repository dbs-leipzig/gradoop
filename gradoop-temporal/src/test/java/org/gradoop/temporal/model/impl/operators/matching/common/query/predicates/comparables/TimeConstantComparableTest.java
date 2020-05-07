package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeConstant;

import static org.junit.Assert.assertEquals;

public class TimeConstantComparableTest {

    @Test
    public void timeConstantTest(){
        TimeConstant c = new TimeConstant(1000L);
        TimeConstantComparable wrapper = new TimeConstantComparable(c);

        // evaluate on embedding
        EmbeddingTPGM embedding = new EmbeddingTPGM();
        EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();

        assertEquals(1000L, wrapper.evaluate(embedding, metaData).getLong());

        // evaluate on temporal element
        TemporalVertex vertex = new TemporalVertexFactory().createVertex();
        assertEquals(1000L, wrapper.evaluate(vertex).getLong());
    }
}
