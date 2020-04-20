package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractExpandColumnFromEdgeTest {
    @Test
    public void testSelectIdOfSpecifiedEmbeddingEntry() throws Exception {
        GradoopId a = GradoopId.get();
        GradoopId b = GradoopId.get();
        GradoopId c = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(a);
        embedding.add(b);
        embedding.add(c);

        ExtractExpandColumnFromEdge selector = new ExtractExpandColumnFromEdge(0);
        assertEquals(a, selector.getKey(embedding));

        selector = new ExtractExpandColumnFromEdge(1);
        assertEquals(b, selector.getKey(embedding));

        selector = new ExtractExpandColumnFromEdge(2);
        assertEquals(c, selector.getKey(embedding));
    }
}
