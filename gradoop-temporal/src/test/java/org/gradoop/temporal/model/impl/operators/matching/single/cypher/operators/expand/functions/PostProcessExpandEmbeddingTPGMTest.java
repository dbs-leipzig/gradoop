package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions.PostProcessExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.ExpandEmbedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostProcessExpandEmbeddingTPGMTest {
    private final GradoopId a = GradoopId.get();
    private final GradoopId b = GradoopId.get();
    private final GradoopId c = GradoopId.get();

    private final Long[] aTime = {0L, 100L, 0L, 100L};
    private final Long[] bTime = {10L, 100L, 10L, 100L};
    private final Long[] cTime = {20L, 110L, 20L, 110L};

    private final Long[] lastEdgeTime = {20L, 115L, 25L, 115L};

    private ExpandEmbeddingTPGM expandEmbedding() {
        EmbeddingTPGM base = new EmbeddingTPGM();
        base.add(a);
        base.add(b);
        base.add(c);
        base.addTimeData(aTime[0], aTime[1], aTime[2], aTime[3]);
        base.addTimeData(bTime[0], bTime[1], bTime[2], bTime[3]);
        base.addTimeData(cTime[0], cTime[1], cTime[2], cTime[3]);

        return new ExpandEmbeddingTPGM(
                base, new GradoopId[]{GradoopId.get(), GradoopId.get(), GradoopId.get(), a}, lastEdgeTime,
                aTime, 20L, 100L, 25L, 100L);
    }


    @Test
    public void testReturnNothingForFalseCircles() throws Exception {
        List<EmbeddingTPGM> result = new ArrayList<>();
        new PostProcessExpandEmbeddingTPGM(0, 2).flatMap(
                expandEmbedding(), new ListCollector<>(result));
        assertEquals(0, result.size());
    }

    @Test
    public void testDoTransformationForClosedCircles() throws Exception {
        List<EmbeddingTPGM> result = new ArrayList<>();
        new PostProcessExpandEmbeddingTPGM(0, 0).
                flatMap(expandEmbedding(), new ListCollector<>(result));
        assertEquals(1, result.size());
    }

    @Test
    public void testReturnNothingForShortsResults() throws Exception {
        List<EmbeddingTPGM> result = new ArrayList<>();
        new PostProcessExpandEmbeddingTPGM(3, -1).
                flatMap(expandEmbedding(), new ListCollector<>(result));
        assertEquals(0, result.size());
    }

    @Test
    public void testDoTransformationForResultsThatFitLowerBound() throws Exception {
        List<EmbeddingTPGM> result = new ArrayList<>();
        new PostProcessExpandEmbeddingTPGM(2, -1).
                flatMap(expandEmbedding(), new ListCollector<>(result));
        assertEquals(1, result.size());
    }
}
