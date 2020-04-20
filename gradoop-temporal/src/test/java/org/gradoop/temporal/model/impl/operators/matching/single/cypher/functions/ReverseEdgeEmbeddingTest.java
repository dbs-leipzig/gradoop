package org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions.ReverseEdgeEmbeddingTPGM;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ReverseEdgeEmbeddingTest {
    @Test
    public void testReversingAnEdgeEmbedding() throws Exception {
        GradoopId a = GradoopId.get();
        GradoopId e = GradoopId.get();
        GradoopId b = GradoopId.get();

        Long[] aTime = new Long[]{1L, 2L, 3L, 4L};
        Long[] eTime = new Long[]{2L, 4L, 8L, 16L};
        Long[] bTime = new Long[]{5L, 10L, 5L, 10L};

        EmbeddingTPGM edge = new EmbeddingTPGM();
        edge.add(a);
        edge.add(e);
        edge.add(b);
        edge.addTimeData(aTime[0], aTime[1], aTime[2], aTime[3]);
        edge.addTimeData(eTime[0], eTime[1], eTime[2], eTime[3]);
        edge.addTimeData(bTime[0], bTime[1], bTime[2], bTime[3]);

        ReverseEdgeEmbeddingTPGM op = new ReverseEdgeEmbeddingTPGM();

        EmbeddingTPGM reversed = op.map(edge);

        assertEquals(b, reversed.getId(0));
        assertEquals(e, reversed.getId(1));
        assertEquals(a, reversed.getId(2));
        assertArrayEquals(bTime, reversed.getTimes(0));
        assertArrayEquals(eTime, reversed.getTimes(1));
        assertArrayEquals(aTime, reversed.getTimes(2));
    }
}
