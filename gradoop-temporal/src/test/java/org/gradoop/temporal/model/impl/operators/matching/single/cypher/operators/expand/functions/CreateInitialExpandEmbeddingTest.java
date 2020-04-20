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

public class CreateInitialExpandEmbeddingTest {
    private final GradoopId m = GradoopId.get();
    private final Long[] mTime = new Long[]{10L, 50L, 10L, 50L};
    private final GradoopId n = GradoopId.get();
    private final Long[] nTime = new Long[]{20L, 50L, 10L, 50L};
    // aCorrect fulfills the time constraints below, aIncorrect does not
    private final GradoopId aCorrect = GradoopId.get();
    private final Long[] aCorrectTime = new Long[]{30L, 50L, 10L, 50L};
    private final GradoopId aIncorrect = GradoopId.get();
    private final Long[] aIncorrectTime = new Long[]{10L, 50L, 10L, 50L};

    private final GradoopId e0 = GradoopId.get();
    private final Long[] e0Time = new Long[]{30L, 50L, 10L, 50L};
    private final GradoopId e1 = GradoopId.get();
    private final Long[] e1Time = new Long[]{30L, 50L, 10L, 50L};

    //temporal criteria for expansion: tx_from values in vertices ascending and
    // transaction lifetime at least 10 ms
    ExpansionCriteria criteria = new ExpansionCriteria(x -> x.f1 >= x.f0, null, null,
            null, null, null, null,
            null, x->x>=10, null);

    // Homomorphism
    @Test
    public void testWithoutDuplicates() throws Exception {
        // time conditions fulfilled
        testJoin(buildEdge(n, e1, aCorrect, nTime, e1Time, aCorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
        // time conditions not fulfilled
        testJoin(buildEdge(n, e1, aIncorrect, nTime, e1Time, aIncorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, false);
    }

    @Test
    public void testHomomorphismWithDuplicateBaseVertex() throws Exception {
        testJoin(buildEdge(n, e1, m, nTime, e1Time, nTime), new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(n, e1, n, nTime, e1Time, nTime), new ArrayList<>(), new ArrayList<>(), -1, true);
    }

    @Test
    public void testHomomorphismWithDuplicateBaseEdge() throws Exception {
        testJoin(buildEdge(n, e0, aCorrect, nTime, e0Time, aCorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, true);
        testJoin(buildEdge(n, e0, aIncorrect, nTime, e0Time, aIncorrectTime),
                new ArrayList<>(), new ArrayList<>(), -1, false);
    }

    @Test
    public void testHomomorphismWithLoop() throws Exception {
        // fails due to time constraint violation
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), new ArrayList<>(), 0, false);
        // temporarily change criteria to impose no restrictions:
        ExpansionCriteria oldCriteria = criteria;
        criteria = new ExpansionCriteria();
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), new ArrayList<>(), 0, true);
        criteria = oldCriteria;
    }

    //VertexIsomorphism
    @Test
    public void testVertexIsomorphismWithoutDuplicates() throws Exception {
        testJoin(buildEdge(n, e1, aCorrect, nTime, e1Time, aCorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, true);
        testJoin(buildEdge(n, e1, aIncorrect, nTime, e1Time, aIncorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithDuplicateBaseVertex() throws Exception {
        // n->m violates time constraints. Thus delete constraints temporarily to check isomorphism correctness
        ExpansionCriteria oldCriteria = criteria;
        criteria = new ExpansionCriteria();
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime), Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
        criteria = oldCriteria;

        testJoin(buildEdge(n, e1, n, nTime, e1Time, nTime), Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithDuplicateBaseEdge() throws Exception {
        testJoin(buildEdge(n, e0, aCorrect, nTime, e0Time, aCorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, true);
        testJoin(buildEdge(n, e0, aIncorrect, nTime, e0Time, aIncorrectTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), -1, false);
    }

    @Test
    public void testVertexIsomorphismWithLoop() throws Exception {
        //fails due to time conditions
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 0, false);
        // temporarily suspend time conditions
        ExpansionCriteria oldCriteria = criteria;
        criteria = new ExpansionCriteria();
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 0, true);
        criteria = oldCriteria;

        testJoin(buildEdge(n, e1, n, nTime, e1Time, nTime),
                Lists.newArrayList(0, 2), new ArrayList<>(), 2, true);
    }

    //EdgeIsomorphism
    @Test
    public void testEdgeIsomorphismWithoutDuplicates() throws Exception {
        testJoin(buildEdge(n, e1, aCorrect, nTime, e1Time, aCorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
        testJoin(buildEdge(n, e1, aIncorrect, nTime, e1Time, aIncorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
    }

    @Test
    public void testEdgeIsomorphismWithDuplicateBaseVertex() throws Exception {
        // fails due to time conditions
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
        // temporarily suspend the time conditions
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria();
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
        criteria = old;

        testJoin(buildEdge(n, e1, n, nTime, e1Time, nTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, true);
    }

    @Test
    public void testEdgeIsomorphismWithDuplicateBaseEdge() throws Exception {
        testJoin(buildEdge(n, e0, aCorrect, nTime, e0Time, aCorrectTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), -1, false);
    }

    @Test
    public void testEdgeIsomorphismWithLoop() throws Exception {
        // fails due to time conditions
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, false);

        //temporarily suspend time conditions
        ExpansionCriteria old = criteria;
        criteria = new ExpansionCriteria();
        testJoin(buildEdge(n, e1, m, nTime, e1Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, true);
        testJoin(buildEdge(n, e0, m, nTime, e0Time, mTime),
                new ArrayList<>(), Lists.newArrayList(0, 1), 0, false);
        criteria = old;
    }


    /**
     * Tests the join of the edge with an Embedding
     * (m, e0, n) x edge
     *
     * @param edge the edge the join is performed with
     * @param distinctVertices distinct vertex columns of the base embedding
     * @param distinctEdges distinct edge columns of the base embedding
     * @param closingColumn closing column
     * @param isResult if true it is expected that the join yields exactly one result, 0 otherwise
     * @throws Exception on failure
     */
    private void testJoin(EmbeddingTPGM edge, List<Integer> distinctVertices,
                          List<Integer> distinctEdges, int closingColumn, boolean isResult) throws Exception {

        EmbeddingTPGM base = new EmbeddingTPGM();
        base.add(m);
        base.add(e0);
        base.add(n);
        base.addTimeData(mTime[0], mTime[1], mTime[2], mTime[3]);
        base.addTimeData(e0Time[0], e0Time[1], e0Time[2], e0Time[3]);
        base.addTimeData(nTime[0], nTime[1], nTime[2], nTime[3]);

        TemporalEdgeWithTiePoint edgeTuple = new TemporalEdgeWithTiePoint(edge);

        CreateExpandEmbeddingTPGM op = new CreateExpandEmbeddingTPGM(
                distinctVertices, distinctEdges, closingColumn, criteria);

        List<ExpandEmbeddingTPGM> results = new ArrayList<>();
        op.join(base, edgeTuple, new ListCollector<>(results));

        if (isResult) {
            assertEquals(base, results.get(0).getBase());
            assertArrayEquals(new GradoopId[]{edge.getId(1)}, results.get(0).getPath());
            assertEquals(edge.getId(2), results.get(0).getEndVertex());
            assertArrayEquals(edge.getTimes(2), results.get(0).getEndVertexTimeData());
            assertArrayEquals(edge.getTimes(1), results.get(0).getLastEdgeTimeData());
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
