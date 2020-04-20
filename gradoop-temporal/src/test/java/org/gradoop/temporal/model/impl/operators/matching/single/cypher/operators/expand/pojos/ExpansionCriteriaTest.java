package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions.util.SerializableFunction;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExpansionCriteriaTest {

    @Test
    public void testCheckSingleCriterionOnExpansion(){
        ExpandEmbeddingTPGM embedding = createExpandEmbedding(new Long[]{10L,100L, 10L, 50L},
                new Long[]{0L, 120L, 5L, 45L}, 10L, 100L,
                10L, 45L);


        // tx_from condition : ascending tx_from in vertices
        ExpansionCriteria txFromVertex = new ExpansionCriteria(x -> x.f0 < x.f1, null,
                null, null, null,
                null, null, null, null,null);
        // fulfills the condition:
        TemporalEdgeWithTiePoint t1 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{10L, 120L, 5L, 45L});
        assertTrue(txFromVertex.checkExpansion(embedding, t1));
        // does not fulfill the condition
        TemporalEdgeWithTiePoint t2 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(txFromVertex.checkExpansion(embedding, t2));

        // tx_to condition : descending tx_to in vertices
        ExpansionCriteria txToVertex = new ExpansionCriteria(null, x -> x.f0 > x.f1,
                null, null, null,
                null, null, null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 110L, 5L, 45L});
        assertTrue(txToVertex.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 150L, 5L, 45L});
        assertFalse(txToVertex.checkExpansion(embedding, t2));

        // val_from condition : identical val_from in vertices
        ExpansionCriteria valFromVertex = new ExpansionCriteria(null, null,
                x -> x.f0==x.f1, null, null,
                null, null, null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(valFromVertex.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 120L, 42L, 45L});
        assertFalse(valFromVertex.checkExpansion(embedding, t2));

        // val_to condition : exact 10 ms tx_to between vertices
        ExpansionCriteria valToVertex = new ExpansionCriteria(null, null,
                null, x -> (x.f1 == (x.f0+10L)), null,
                null, null, null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{10L, 120L, 5L, 55L});
        assertTrue(valToVertex.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L,100L, 10L, 50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(valToVertex.checkExpansion(embedding, t2));


        // tx_from condition : ascending tx_from (delta >=5) in edges
        ExpansionCriteria txFromEdge = new ExpansionCriteria(null, null,
                null, null, x -> (x.f1 >= (x.f0+5)),
                null, null, null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{16L, 100L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(txFromEdge.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{11L, 100L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(txFromEdge.checkExpansion(embedding, t2));

        // tx_to condition : ascending tx_to (delta <= 20) in edges
        ExpansionCriteria txToEdge = new ExpansionCriteria(null, null,
                null, null, null,
                x -> (x.f1>=x.f0 && (x.f1-x.f0)<=20), null, null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L, 111L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(txToEdge.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L, 142L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(txToEdge.checkExpansion(embedding, t2));

        // val_from condition : descending val_from (delta=10ms) in edges
        ExpansionCriteria valFromEdge = new ExpansionCriteria(null, null,
                null, null, null,
                null, x -> ((x.f0-x.f1)==10), null, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L, 100L,0L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(valFromEdge.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L, 100L,3L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(valFromEdge.checkExpansion(embedding, t2));

        // val_to condition : val_to not identical in successive edges
        ExpansionCriteria valToEdge = new ExpansionCriteria(null, null,
                null, null, null,
                null, null, x-> x.f0!=x.f1, null,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L, 100L,10L,42L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(valToEdge.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L, 100L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(valToEdge.checkExpansion(embedding, t2));


        //tx-lifetime condition: tx-lifetime of whole >= 50 ms
        ExpansionCriteria txLifetime = new ExpansionCriteria(null, null,
                null, null, null,
                null, null, null,
                x -> x>=50,null);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L, 100L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(txLifetime.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{1L, 2L,10L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(txLifetime.checkExpansion(embedding, t2));

        //valid-lifetime condition: tx-lifetime of whole between 10 and 20ms
        ExpansionCriteria valLifetime = new ExpansionCriteria(null, null,
                null, null, null,
                null, null, null,
                null,x -> x>=10 && x<=20);
        // fulfills the condition:
        t1 = createExpansionEdge(new Long[]{10L, 100L,30L,45L}, new Long[]{0L, 120L, 5L, 45L});
        assertTrue(valLifetime.checkExpansion(embedding, t1));
        // does not fulfill the condition
        t2 = createExpansionEdge(new Long[]{10L, 100L,3L,50L}, new Long[]{0L, 120L, 5L, 45L});
        assertFalse(valLifetime.checkExpansion(embedding, t2));

        // empty criteria should -> every path should pass
        ExpansionCriteria empty = new ExpansionCriteria(null, null,
                null, null, null,
                null, null, null,
                null,null);
        assertTrue(empty.checkExpansion(embedding, t1));
        assertTrue(empty.checkExpansion(embedding,t2));
    }

    @Test
    public void testCheckMultipleConditionsOnExpansion(){
        ExpandEmbeddingTPGM embedding = createExpandEmbedding(new Long[]{10L,100L, 10L, 50L},
                new Long[]{0L, 120L, 5L, 45L}, 10L, 100L,
                10L, 45L);
        // a more complex constraint set:
        // vertex tx_from strictly ascending
        // edge valid_to ascending with delta max. 5
        ExpansionCriteria criteria = new ExpansionCriteria(x -> x.f1>x.f0, null,
                null, null, null,
                null, null, x -> (x.f1 >= x.f0 && (x.f1-x.f0)<=5),
                null,null);
        //fulfills both conditions
        TemporalEdgeWithTiePoint e1 = createExpansionEdge(new Long[]{10L, 100L, 10L, 53L},
                new Long[]{5L, 120L, 5L, 45L});
        assertTrue(criteria.checkExpansion(embedding, e1));

        //fulfill one condition
        TemporalEdgeWithTiePoint e2 = createExpansionEdge(new Long[]{10L, 100L, 10L, 72L},
                new Long[]{5L, 120L, 5L, 45L});
        TemporalEdgeWithTiePoint e3 = createExpansionEdge(new Long[]{10L, 100L, 10L, 53L},
                new Long[]{0L, 120L, 5L, 45L});
        assertFalse(criteria.checkExpansion(embedding, e2));
        assertFalse(criteria.checkExpansion(embedding, e3));

        //fulfills neither condition
        TemporalEdgeWithTiePoint e4 = createExpansionEdge(new Long[]{10L, 120L, 10L, 72L},
                new Long[]{0L, 120L, 5L, 45L});
        assertFalse(criteria.checkExpansion(embedding, e4));

    }

    @Test
    public void testCheckSingleCriterionOnEdge(){
        // condition used throughout the tests on vertices: strictly descending
        SerializableFunction<Tuple2<Long, Long>, Boolean> strictDesc = x -> x.f0>x.f1;
        // tx_from condition on vertices
        ExpansionCriteria criteria = new ExpansionCriteria(strictDesc,
                null, null,
                null, null, null,
                null, null, null,
                null);
        // fulfills the condition
        TemporalEdgeWithTiePoint e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{2L, 60L, 20L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        TemporalEdgeWithTiePoint e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{42L, 60L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        // tx_to condition on vertices
        criteria = new ExpansionCriteria(null,
                strictDesc, null,
                null, null, null,
                null, null, null,
                null);
        // fulfills the condition
        e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{0L, 42L, 20L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{40L, 60L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        // valid_from condition on vertices
        criteria = new ExpansionCriteria(null,
                null, strictDesc,
                null, null, null,
                null, null, null,
                null);
        // fulfills the condition
        e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{0L, 40L, 7L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{40L, 60L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        // valid_to condition on vertices
        criteria = new ExpansionCriteria(null,
                null, null,
                strictDesc, null, null,
                null, null, null,
                null);
        // fulfills the condition
        e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{0L, 40L, 20L, 32L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{40L, 60L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        //condition for lifetime tests: lifetime>=10 ms
        // tx lifetime
        SerializableFunction<Long, Boolean> lifeCond = x -> x>=10;
        criteria = new ExpansionCriteria(null,
                null, null,
                null, null, null,
                null, null, lifeCond,
                null);
        // fulfills the condition
        e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{0L, 40L, 20L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{17L,20L, 10L, 30L}, new Long[]{0L, 40L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        // valid lifetime
        criteria = new ExpansionCriteria(null,
                null, null,
                null, null, null,
                null, null, null,
                lifeCond);
        // fulfills the condition
        e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{0L, 40L, 20L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));
        // does not fulfill the condition
        e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{17L,20L, 10L, 30L}, new Long[]{0L, 40L, 42L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));

        // empty criteria -> every edge should pass
        criteria = new ExpansionCriteria(null,
                null, null,
                null, null, null,
                null, null, null,
                null);
        assertTrue(criteria.checkInitialEdge(e1));
        assertTrue(criteria.checkInitialEdge(e2));
    }

    @Test
    public void checkMultipleConditionsOnEdge(){
        // multiple criteria: vertex tx_from strictly ascending, valid lifetime at least 10 ms
        ExpansionCriteria criteria = new ExpansionCriteria(x -> x.f0<x.f1,
                null, null,
                null, null, null,
                null, null, null,
                x -> x>=10);

        // fulfills both conditions
        TemporalEdgeWithTiePoint e1 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{10L, 60L, 20L, 60L});
        assertTrue(criteria.checkInitialEdge(e1));

        // fulfill just one condition
        TemporalEdgeWithTiePoint e2 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{10L, 60L, 42L, 60L});
        TemporalEdgeWithTiePoint e3 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{2L, 60L, 20L, 60L});
        assertFalse(criteria.checkInitialEdge(e2));
        assertFalse(criteria.checkInitialEdge(e3));

        // fulfills no condition
        TemporalEdgeWithTiePoint e4 = createInitEdge(new Long[]{5L, 50L, 10L, 40L},
                new Long[]{10L,20L, 10L, 30L}, new Long[]{2L, 60L, 42L, 60L});
        assertFalse(criteria.checkInitialEdge(e4));
    }


    /**
     * Creates an ExpandEmbedding containing one hop for testing
     * @param edgeTime time data of the edge on the path
     * @param vertexTime time data of the end node of the path
     * @param maxTxFrom maximum tx_from value on the path
     * @param minTxTo minimum tx_to value on the path
     * @param maxValFrom maximum val_from value on the path
     * @param minValTo minimum val_to value on the path
     * @return ExpandEmbedding containing one hop
     */
    private ExpandEmbeddingTPGM createExpandEmbedding(Long[] edgeTime, Long[] vertexTime,
                                                      Long maxTxFrom, Long minTxTo,
                                                      Long maxValFrom, Long minValTo){
        EmbeddingTPGM base = new EmbeddingTPGM();
        base.add(GradoopId.get());

        GradoopId[] path = new GradoopId[]{GradoopId.get(), GradoopId.get()};

        return new ExpandEmbeddingTPGM(base, path, edgeTime, vertexTime, maxTxFrom,
                minTxTo, maxValFrom, minValTo);
    }

    /**
     * Creates a TemporalEdgeWithTiePoint with irrelevant source time data. Only used for
     * testing an expansion, where the implementation ignores the TemporalEdgeWithTiePoint's
     * source time data.
     * @param edgeTime edge time data.
     * @param targetTime target vertex time data.
     * @return TemporalEdgeWithTiePoint with some arbitrary source time data.
     */
    private TemporalEdgeWithTiePoint createExpansionEdge(Long[] edgeTime, Long[] targetTime){
        return createInitEdge(new Long[]{1L,2L,1L,2L}, edgeTime, targetTime);
    }

    /**
     * Creates a TemporalEdgeWithTiePoint with the specified time data
     * @param sourceTime source time data
     * @param edgeTime edge time data
     * @param targetTime target time data
     * @return TemporalEdgeWithTiePoint with the specified time data
     */
    private TemporalEdgeWithTiePoint createInitEdge(Long[] sourceTime, Long[] edgeTime,
                                                    Long[] targetTime){
        EmbeddingTPGM emb = new EmbeddingTPGM();
        emb.addAll(GradoopId.get(), GradoopId.get(), GradoopId.get());
        emb.addTimeData(sourceTime[0], sourceTime[1], sourceTime[2], sourceTime[3]);
        emb.addTimeData(edgeTime[0], edgeTime[1], edgeTime[2], edgeTime[3]);
        emb.addTimeData(targetTime[0], targetTime[1], targetTime[2], targetTime[3]);
        return new TemporalEdgeWithTiePoint(emb);
    }

}
