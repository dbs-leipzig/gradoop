package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.AdoptEmptyPaths;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class AdoptEmptyPathsTest {
    @Test
    public void testFilterEmbeddingsOnClosingColumn() throws Exception {
        GradoopId a = GradoopId.get();
        GradoopId b = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(a);
        embedding.add(b);
        embedding.addTimeData(1L, 2L, 3L, 4L);

        List<EmbeddingTPGM> result = new ArrayList<>();
        new AdoptEmptyPaths(1,0,  0).flatMap(
                embedding, new ListCollector<>(result));
        assertTrue(result.isEmpty());


        new AdoptEmptyPaths(1, 0,1).flatMap(
                embedding, new ListCollector<>(result));
        assertEquals(1, result.size());
        assertArrayEquals(new Long[]{1L, 2L, 3L, 4L}, result.get(0).getTimes(0));
    }

    @Test
    public void testEmbeddingFormat() throws Exception {
        GradoopId a = GradoopId.get();
        GradoopId b = GradoopId.get();

        EmbeddingTPGM embedding = new EmbeddingTPGM();
        embedding.add(a);
        embedding.add(b);
        embedding.addTimeData(1L, 2L, 3L, 4L);

        List<EmbeddingTPGM> result = new ArrayList<>();
        new AdoptEmptyPaths(1, 0,-1).flatMap(embedding, new ListCollector<>(result));

        assertTrue(result.get(0).getIdList(2).isEmpty());
        assertEquals(b, result.get(0).getId(3));
    }
}
