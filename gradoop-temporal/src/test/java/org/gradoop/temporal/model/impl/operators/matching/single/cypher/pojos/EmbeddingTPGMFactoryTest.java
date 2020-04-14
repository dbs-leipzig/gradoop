package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class EmbeddingTPGMFactoryTest {

    @Test
    public void testFromVertex() throws Exception {
        Properties properties = new Properties();
        properties.set("foo", 1);
        properties.set("bar", "42");
        properties.set("baz", false);
        TemporalVertex vertex = new TemporalVertexFactory().createVertex("TestVertex", properties);
        Tuple2<Long, Long> tx = new Tuple2<>(123L, 124L);
        vertex.setTransactionTime(tx);
        Tuple2<Long, Long> val = new Tuple2<>(123456L, 12456787L);
        vertex.setValidTime(val);

        EmbeddingTPGM embedding =
                EmbeddingTPGMFactory.fromVertex(vertex, Lists.newArrayList("foo", "bar"));

        assertEquals(1, embedding.size());
        assertEquals(vertex.getId(), embedding.getId(0));
        assertEquals(PropertyValue.create(1), embedding.getProperty(0));
        assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
        Long[] times = embedding.getTimes(0);
        assertEquals(tx.f0, times[0]);
        assertEquals(tx.f1, times[1]);
        assertEquals(val.f0, times[2]);
        assertEquals(val.f1, times[3]);
    }

    @Test
    public void testFromEdge() throws Exception {
        Properties properties = new Properties();
        properties.set("foo", 1);
        properties.set("bar", "42");
        properties.set("baz", false);
        TemporalEdge edge = new TemporalEdgeFactory().createEdge(
                "TestVertex", GradoopId.get(), GradoopId.get(), properties
        );
        Tuple2<Long, Long> tx = new Tuple2<>(1253453L, 124346557L);
        edge.setTransactionTime(tx);
        Tuple2<Long, Long> val = new Tuple2<>(13456L, 127834487L);
        edge.setValidTime(val);

        EmbeddingTPGM embedding =
                EmbeddingTPGMFactory.fromEdge(edge, Lists.newArrayList("foo", "bar"), false);

        assertEquals(3, embedding.size());
        assertEquals(edge.getSourceId(), embedding.getId(0));
        assertEquals(edge.getId(), embedding.getId(1));
        assertEquals(edge.getTargetId(), embedding.getId(2));
        assertEquals(PropertyValue.create(1), embedding.getProperty(0));
        assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
        Long[] times = embedding.getTimes(0);
        assertEquals(tx.f0, times[0]);
        assertEquals(tx.f1, times[1]);
        assertEquals(val.f0, times[2]);
        assertEquals(val.f1, times[3]);
    }

    @Test
    public void testFromTriple(){
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

        EmbeddingTPGM embedding = EmbeddingTPGMFactory.fromTriple(triple, new ArrayList<String>(), new ArrayList<String>(),
                new ArrayList<String>(), "a", "b");

        Long[] timecol0 = embedding.getTimes(0);
        assertEquals(source_tx.f0, timecol0[0]);
        assertEquals(source_tx.f1, timecol0[1]);
        assertEquals(source_val.f0, timecol0[2]);
        assertEquals(source_val.f1, timecol0[3]);

        Long[] timecol1 = embedding.getTimes(1);
        assertEquals(edge_tx.f0, timecol1[0]);
        assertEquals(edge_tx.f1, timecol1[1]);
        assertEquals(edge_val.f0, timecol1[2]);
        assertEquals(edge_val.f1, timecol1[3]);

        Long[] timecol2 = embedding.getTimes(2);
        assertEquals(target_tx.f0, timecol2[0]);
        assertEquals(target_tx.f1, timecol2[1]);
        assertEquals(target_val.f0, timecol2[2]);
        assertEquals(target_val.f1, timecol2[3]);
    }

    @Test
    public void testFromTripleSelfLoop(){
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

        EmbeddingTPGM embedding = EmbeddingTPGMFactory.fromTriple(triple, new ArrayList<String>(), new ArrayList<String>(),
                new ArrayList<String>(), "a", "a");

        Long[] timecol0 = embedding.getTimes(0);
        assertEquals(source_tx.f0, timecol0[0]);
        assertEquals(source_tx.f1, timecol0[1]);
        assertEquals(source_val.f0, timecol0[2]);
        assertEquals(source_val.f1, timecol0[3]);

        Long[] timecol1 = embedding.getTimes(1);
        assertEquals(edge_tx.f0, timecol1[0]);
        assertEquals(edge_tx.f1, timecol1[1]);
        assertEquals(edge_val.f0, timecol1[2]);
        assertEquals(edge_val.f1, timecol1[3]);

        // no third element should be there
        try{
            Long[] timecol2 = embedding.getTimes(2);
            fail();
        }
        catch (IndexOutOfBoundsException e){

        }
    }
}
