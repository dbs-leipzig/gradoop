package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TripleTPGMTest {

    @Test
    public void testValidTriple(){
        GradoopId sId = GradoopId.get();
        TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");
        Tuple2<Long, Long> source_tx = new Tuple2<>(1253453L, 124346557L);
        Tuple2<Long, Long> source_val = new Tuple2<>(2443L, 2879979L);
        source.setTransactionTime(source_tx);
        source.setValidTime(source_val);

        GradoopId tId = GradoopId.get();
        TemporalVertex target = new TemporalVertexFactory().initVertex(tId,"target");
        Tuple2<Long, Long> target_tx = new Tuple2<>(12453L, 12434643557L);
        Tuple2<Long, Long> target_val = new Tuple2<>(57833L, 98989979L);
        target.setTransactionTime(target_tx);
        target.setValidTime(target_val);

        TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, tId);
        Tuple2<Long, Long> edge_tx = new Tuple2<>(456L, 1234567L);
        Tuple2<Long, Long> edge_val = new Tuple2<>(9867L, 121212121L);
        edge.setTransactionTime(edge_tx);
        edge.setValidTime(edge_val);

        TripleTPGM triple = new TripleTPGM(source, edge, target);

        assertEquals(triple.f0, source);
        assertEquals(triple.f1, edge);
        assertEquals(triple.f2, target);
    }

    @Test
    public void testValidTripleSelfLoop(){
        GradoopId sId = GradoopId.get();
        TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");
        Tuple2<Long, Long> source_tx = new Tuple2<>(1253453L, 124346557L);
        Tuple2<Long, Long> source_val = new Tuple2<>(2443L, 2879979L);
        source.setTransactionTime(source_tx);
        source.setValidTime(source_val);

        TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, sId);
        Tuple2<Long, Long> edge_tx = new Tuple2<>(456L, 1234567L);
        Tuple2<Long, Long> edge_val = new Tuple2<>(9867L, 121212121L);
        edge.setTransactionTime(edge_tx);
        edge.setValidTime(edge_val);

        TripleTPGM triple = new TripleTPGM(source, edge, source);

        assertEquals(triple.f0, source);
        assertEquals(triple.f1, edge);
        assertEquals(triple.f2, triple.f0);
    }

    @Test (expected =  IllegalArgumentException.class)
    public void testInvalidSourceTriple(){

        GradoopId sId = GradoopId.get();
        TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");

        GradoopId tId = GradoopId.get();
        TemporalVertex target = new TemporalVertexFactory().initVertex(tId,"target");

        // wrong source ID
        TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", GradoopId.get(), tId);

        TripleTPGM triple = new TripleTPGM(source, edge, target);
    }

    @Test (expected =  IllegalArgumentException.class)
    public void testInvalidTargetTriple(){

        GradoopId sId = GradoopId.get();
        TemporalVertex source = new TemporalVertexFactory().initVertex(sId, "source");

        GradoopId tId = GradoopId.get();
        TemporalVertex target = new TemporalVertexFactory().initVertex(tId,"target");

        // wrong target ID
        TemporalEdge edge = new TemporalEdgeFactory().createEdge("edge", sId, GradoopId.get());

        TripleTPGM triple = new TripleTPGM(source, edge, target);
    }
}
