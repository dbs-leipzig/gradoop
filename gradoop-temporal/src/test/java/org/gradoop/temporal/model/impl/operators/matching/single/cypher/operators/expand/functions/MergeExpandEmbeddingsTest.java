package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpandEmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.ExpansionCriteria;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MergeExpandEmbeddingsTest {
    private final GradoopId m = GradoopId.get();
    private final Long[] mTime = new Long[]{0L, 50L, 0L, 50L};
    private final GradoopId n = GradoopId.get();
    private final Long[] nTime = new Long[]{10L, 60L, 10L, 60L};
    private final GradoopId a = GradoopId.get();
    private final Long[] aTime = new Long[]{20L, 60L, 20L, 60L};
    // bCorrect fulfills the temporal conditions, bIncorrect does not
    private final GradoopId bCorrect = GradoopId.get();
    private final Long[] bCorrectTime = new Long[]{30L, 70L, 30L, 70L};
    private final GradoopId bIncorrect = GradoopId.get();
    private final Long[] bIncorrectTime = new Long[]{10L, 25L, 30L, 70L};

    private final GradoopId e0 = GradoopId.get();
    private final Long[] e0Time = new Long[]{0L, 60L, 0L, 60L};
    private final GradoopId e1 = GradoopId.get();
    private final Long[] e1Time = new Long[]{10L, 60L, 10L, 60L};
    private final GradoopId e2 = GradoopId.get();
    private final Long[] e2Time = new Long[]{20L, 70L, 20L, 70L};

    //temporal criteria for expansion: tx_from values in vertices ascending and
    // transaction lifetime at least 10 ms
    ExpansionCriteria criteria = new ExpansionCriteria(x -> x.f1 >= x.f0, null, null,
            null, null, null, null,
            null, x->x>=10, null);

    // Homomorphism
    @Test
    public void testWithoutDuplicates() throws Exception {
        testJoin(buildEdge(a, e2, bCorrect, aTime, e2Time, bCorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e2, bIncorrect, aTime, e2Time, bIncorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, false);
    }

    @Test
    public void testHomomorphismWithDuplicateBaseVertex() throws Exception {
        // relax time conditions in order to allow loops
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, x->x>=10, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime), new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e2, m, aTime, e2Time, nTime), new ArrayList<>(), new ArrayList<>(), -1, true);

        criteria = old;
    }

    @Test
    public void testHomomorphismWithDuplicatePathVertex() throws Exception {
        testJoin(buildEdge(a, e2, a, aTime, e2Time, aTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
    }

    @Test
    public void testHomomorphismWithDuplicateBaseEdge() throws Exception {
        testJoin(buildEdge(a, e0, bCorrect, aTime, e0Time, bCorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e0, bIncorrect, aTime, e0Time, bIncorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, false);
    }

    @Test
    public void testHomomorphismWithDuplicatePathEdge() throws Exception {
        testJoin(buildEdge(a, e1, bCorrect, aTime, e1Time, bCorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e1, bIncorrect, aTime, e1Time, bIncorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, false);
    }

    @Test
    public void testHomomorphismWithLoop() throws Exception {
        // relax time conditions in order to allow loops
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, x->x>=10, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), new ArrayList<>(), 0, true);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, nTime),
                new ArrayList<>(), new ArrayList<>(), 2, true);

        criteria = old;

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), new ArrayList<>(), 0, false);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, nTime),
                new ArrayList<>(), new ArrayList<>(), 2, false);
    }

    //VertexIsomorphism
    @Test
    public void testVertexIsomorphismWithoutDuplicates() throws Exception {
        testJoin(buildEdge(a, e2, bCorrect, aTime, e2Time, bCorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e2, bIncorrect, aTime, e2Time, bIncorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithDuplicateBaseVertex() throws Exception {
        // relax time conditions. Join should fail here, but not due to temporal issues
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, null, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, nTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);

        criteria = old;
    }

    @Test
    public void testVertexIsomorphismWithDuplicatePathVertex() throws Exception {
        // relax time conditions. Join should fail here, but not due to temporal issues
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, null, null);

        testJoin(buildEdge(a, e2, a, aTime, e2Time, aTime), Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);

        criteria = old;
    }

    @Test
    public void testVertexIsomorphismWithDuplicateBaseEdge() throws Exception {
        testJoin(buildEdge(a, e0, bCorrect, aTime, e0Time, bCorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e0, bIncorrect, aTime, e0Time, bIncorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithDuplicatePathEdge() throws Exception {
        testJoin(buildEdge(a, e1, bCorrect, aTime, e1Time, bCorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, true);
        testJoin(buildEdge(a, e1, bIncorrect, aTime, e1Time, bIncorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithLoop() throws Exception {
        // relax time conditions in order to allow loops
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, x->x>=10, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 0, true);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 2, true);

        criteria = old;
        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 0, false);
    }

    //EdgeIsomorphism
    @Test
    public void testEdgeIsomorphismWithoutDuplicates() throws Exception {
        testJoin(buildEdge(a, e2, bCorrect, aTime, e2Time, bCorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
        testJoin(buildEdge(a, e2, bIncorrect, aTime, e2Time, bIncorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
    }

    @Test
    public void testEdgeIsomorphismWithDuplicateBaseVertex() throws Exception {
        // relax time conditions in order to allow loops
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, x->x>=10, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, nTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);

        criteria = old;
        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
        testJoin(buildEdge(a, e2, n, aTime, e2Time, nTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
    }

    @Test
    public void testEdgeIsomorphismWithDuplicatePathVertex() throws Exception {
        testJoin(buildEdge(a, e2, a, aTime, e2Time, aTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
    }

    @Test
    public void testEdgeIsomorphismWithDuplicateBaseEdge() throws Exception {
        // relax time conditions. Join should fail here, but not due to temporal issues
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, null, null);

        testJoin(buildEdge(a, e0, bCorrect, aTime, e0Time, bCorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
        criteria = old;
    }

    @Test
    public void testEdgeIsomorphismWithDuplicatePathEdge() throws Exception {
        testJoin(buildEdge(a, e1, bCorrect, aTime, e1Time, bCorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
    }

    @Test
    public void testEdgeIsomorphismWithLoop() throws Exception {
        // relax time conditions in order to allow loops
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria(null, null, null,
                null, null, null, null,
                null, x->x>=10, null);

        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, true);
        testJoin(buildEdge(a, e1, m, aTime, e1Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, false);
        testJoin(buildEdge(a, e0, m, aTime, e0Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, false);

        criteria = old;
        testJoin(buildEdge(a, e2, m, aTime, e2Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, false);
    }


    /**
     * Tests Join of a base embedding m -e0-> n -e1-> a with another edge
     * @param edge the edge to join
     * @param distinctVertices distinct vertex columns of the base embedding
     * @param distinctEdges distinct edge columns of the base embedding
     * @param closingColumn closing column
     * @param isResult true iff it is expected that the join yields exactly one result
     * @throws Exception on failure
     */
    private void testJoin(EmbeddingTPGM edge, List<Integer> distinctVertices, List<Integer>
            distinctEdges, int closingColumn, boolean isResult) throws Exception {

        EmbeddingTPGM base = new EmbeddingTPGM();
        base.add(m);
        base.add(e0);
        base.add(n);
        base.addTimeData(mTime[0], mTime[1], mTime[2], mTime[3]);
        base.addTimeData(e0Time[0], e0Time[1], e0Time[2], e0Time[3]);
        base.addTimeData(nTime[0], nTime[1], nTime[2], nTime[3]);

        TemporalEdgeWithTiePoint edgeTuple = new TemporalEdgeWithTiePoint(edge);

        Long maxTxFrom = Math.max(Math.max(mTime[0], e0Time[0]),nTime[0]);
        Long minTxTo = Math.min(Math.min(mTime[1], e0Time[1]),nTime[1]);
        Long maxValidFrom = Math.max(Math.max(mTime[2], e0Time[2]),nTime[2]);
        Long minValidTo = Math.min(Math.min(mTime[3], e0Time[3]),nTime[3]);

        ExpandEmbeddingTPGM expandEmbedding = new ExpandEmbeddingTPGM(base, new GradoopId[]{e1, a},
                e1Time, aTime, maxTxFrom, minTxTo, maxValidFrom, minValidTo);

        MergeExpandEmbeddingsTPGM op =
                new MergeExpandEmbeddingsTPGM(distinctVertices, distinctEdges, closingColumn, criteria);

        List<ExpandEmbeddingTPGM> results = new ArrayList<>();
        op.join(expandEmbedding, edgeTuple, new ListCollector<>(results));

        assertEquals(isResult ? 1 : 0, results.size());

        if (isResult) {
            assertArrayEquals(new GradoopId[]{e1, a, edge.getId(1)}, results.get(0).getPath());
            assertEquals(edge.getId(2), results.get(0).getEndVertex());
            assertArrayEquals(edge.getTimes(1), results.get(0).getLastEdgeTimeData());
            assertArrayEquals(edge.getTimes(2), results.get(0).getEndVertexTimeData());
        }
    }

    /**
     * Builds an Embedding with the three entries resembling an edge
     * @param src the edges source id
     * @param edgeId the edges id
     * @param tgt the edges target id
     * @param srcTime source time data
     * @param edgeTime edge time data
     * @param tgtTime target time data
     * @return Embedding resembling the edge
     */
    private EmbeddingTPGM buildEdge(GradoopId src, GradoopId edgeId, GradoopId tgt,
                                    Long[] srcTime, Long[] edgeTime, Long[] tgtTime) {
        EmbeddingTPGM edge = new EmbeddingTPGM();
        edge.add(src);
        edge.add(edgeId);
        edge.add(tgt);
        edge.addTimeData(srcTime[0], srcTime[1], srcTime[2], srcTime[3]);
        edge.addTimeData(edgeTime[0], edgeTime[1], edgeTime[2], edgeTime[3]);
        edge.addTimeData(tgtTime[0], tgtTime[1], tgtTime[2], tgtTime[3]);

        return edge;
    }
}
