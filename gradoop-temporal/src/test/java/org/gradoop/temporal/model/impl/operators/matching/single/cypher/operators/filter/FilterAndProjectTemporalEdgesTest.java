package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectTemporalEdges;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectTemporalEdgesTest extends PhysicalTPGMOperatorTest {
    @Test
    public void testFilterWithNoPredicates() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a]->()");

        TemporalEdgeFactory edgeFactory = new TemporalEdgeFactory();
        Properties properties = Properties.create();
        properties.set("name", "Anton");
        TemporalEdge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);
        e1.setTransactionTime(new Tuple2<>(1234L, 12345L));
        e1.setValidTime(new Tuple2<>(5678L, 56789L));
        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1);

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalEdges(edges, predicates, new ArrayList<>(), false)
                .evaluate()
                .collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(1), e1.getId());
    }

    @Test
    public void testFilterEdgesByProperties() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.since > 2013");

        TemporalEdgeFactory edgeFactory = new TemporalEdgeFactory();
        Properties properties = Properties.create();
        properties.set("since", 2014);
        TemporalEdge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);
        e1.setTransactionTime(new Tuple2<>(1234L, 12345L));
        e1.setValidTime(new Tuple2<>(5678L, 56789L));

        properties = Properties.create();
        properties.set("since", 2013);
        TemporalEdge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);
        e2.setTransactionTime(new Tuple2<>(9876L, 98765L));
        e2.setValidTime(new Tuple2<>(5432L, 54321L));

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalEdges(edges, predicates, new ArrayList<>(), false)
                .evaluate()
                .collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(1), e1.getId());
    }

    @Test
    public void testFilterEdgesByLabel() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

        TemporalEdgeFactory edgeFactory = new TemporalEdgeFactory();
        TemporalEdge e1 = edgeFactory.createEdge("likes", GradoopId.get(), GradoopId.get());
        e1.setTransactionTime(new Tuple2<>(1234L, 12345L));
        e1.setValidTime(new Tuple2<>(5678L, 56789L));
        TemporalEdge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get());
        e2.setTransactionTime(new Tuple2<>(9876L, 98765L));
        e2.setValidTime(new Tuple2<>(5432L, 54321L));
        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalEdges(edges, predicates, new ArrayList<>(), false)
                .evaluate()
                .collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(1), e1.getId());
    }

    @Test
    public void testFilterEdgesByTime() throws Exception {
        TemporalCNF predicates = predicateFromQuery(
                "MATCH ()-[a]->() WHERE a.val_from.before(1970-01-01T00:00:01)");

        TemporalEdgeFactory edgeFactory = new TemporalEdgeFactory();
        // e1 does not fulfill the predicate
        TemporalEdge e1 = edgeFactory.createEdge("likes", GradoopId.get(), GradoopId.get());
        e1.setTransactionTime(new Tuple2<>(1234L, 12345L));
        e1.setValidTime(new Tuple2<>(5678L, 56789L));
        // e2 fulfills the predicate
        TemporalEdge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get());
        e2.setTransactionTime(new Tuple2<>(9876L, 98765L));
        e2.setValidTime(new Tuple2<>(123L, 54321L));
        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalEdges(edges, predicates, new ArrayList<>(), false)
                .evaluate()
                .collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(1), e2.getId());
    }

    @Test
    public void testResultingEntryList() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        TemporalEdge
                edge = new TemporalEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);
        Tuple2<Long,Long> transactionTime = new Tuple2<>(9876L, 98765L);
        Tuple2<Long,Long> validTime = new Tuple2<>(123L, 54321L);
        edge.setTransactionTime(transactionTime);
        edge.setValidTime(validTime);

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(edge);

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalEdges(edges, predicates, new ArrayList<>(), false)
                .evaluate()
                .collect();

        assertEquals(edge.getSourceId(), result.get(0).getId(0));
        assertEquals(edge.getId(),       result.get(0).getId(1));
        assertEquals(edge.getTargetId(), result.get(0).getId(2));
        assertEquals(edge.getValidTime(), validTime);
        assertEquals(edge.getTransactionTime(), transactionTime);
    }

    @Test
    public void testProjectionOfAvailableValues() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        TemporalEdge
                edge = new TemporalEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);
        edge.setTransactionTime(new Tuple2<>(9876L, 98765L));
        edge.setValidTime(new Tuple2<>(123L, 54321L));

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(edge);

        List<String> projectionPropertyKeys = Lists.newArrayList("name");

        EmbeddingTPGM result = new FilterAndProjectTemporalEdges(edges, predicates, projectionPropertyKeys, false)
                .evaluate().collect().get(0);

        assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
    }

    @Test
    public void testProjectionOfMissingValues() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        TemporalEdge
                edge = new TemporalEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);
        edge.setTransactionTime(new Tuple2<>(9876L, 98765L));
        edge.setValidTime(new Tuple2<>(123L, 54321L));

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(edge);

        List<String> projectionPropertyKeys = Lists.newArrayList("name", "since");

        EmbeddingTPGM result = new FilterAndProjectTemporalEdges(edges, predicates, projectionPropertyKeys, false)
                .evaluate().collect().get(0);

        assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
        assertEquals(result.getProperty(1), PropertyValue.NULL_VALUE);
    }

    @Test
    public void testProjectLoop() throws Exception {
        TemporalCNF predicates = predicateFromQuery("MATCH (a)-[b]->(a)");

        GradoopId a = GradoopId.get();
        TemporalEdge edge = new TemporalEdgeFactory().createEdge(a, a);
        edge.setTransactionTime(new Tuple2<>(9876L, 98765L));
        edge.setValidTime(new Tuple2<>(123L, 54321L));

        DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(edge);

        EmbeddingTPGM result = new FilterAndProjectTemporalEdges(edges, predicates, Collections.emptyList(), true)
                .evaluate().collect().get(0);

        assertEquals(result.size(), 2);
        assertEquals(a, result.getId(0));
        assertEquals(edge.getId(), result.getId(1));
    }
}
