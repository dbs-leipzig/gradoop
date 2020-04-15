package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectTemporalTriples;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.TripleTPGM;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FilterAndProjectTemporalTriplesTest extends PhysicalTPGMOperatorTest {
    private TemporalVertexFactory vertexFactory;
    private TemporalEdgeFactory edgeFactory;

    @Before
    public void setUp() throws Exception {
        this.vertexFactory = new TemporalVertexFactory();
        this.edgeFactory = new TemporalEdgeFactory();
    }

    @Test
    public void testFilterWithNoPredicates() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        TemporalVertex a = vertexFactory.createVertex("Person", properties);
        a.setTransactionTime(new Tuple2<>(123L, 1234L));
        a.setValidTime(new Tuple2<>(456L, 4567L));

        properties = Properties.create();
        properties.set("name", "Bob");
        TemporalVertex b = vertexFactory.createVertex("Person", properties);
        b.setTransactionTime(new Tuple2<>(1L, 2L));
        b.setValidTime(new Tuple2<>(3L, 4L));

        properties = Properties.create();
        properties.set("since", "2013");
        TemporalEdge e = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);
        e.setTransactionTime(new Tuple2<>(321L, 4321L));
        e.setValidTime(new Tuple2<>(654L, 7654L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(new TripleTPGM(a, e, b));

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(
                triples,
                "a", "e", "b",
                predicates,
                new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a.getId());
        assertEquals(result.get(0).getId(1), e.getId());
        assertEquals(result.get(0).getId(2), b.getId());
        assertArrayEquals(result.get(0).getTimes(0), new Long[]{123L, 1234L,456L, 4567L});
        assertArrayEquals(result.get(0).getTimes(1), new Long[]{321L, 4321L, 654L, 7654L});
        assertArrayEquals(result.get(0).getTimes(2), new Long[]{1L, 2L, 3L, 4L});}

    @Test
    public void testFilterByProperties() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b) WHERE a.age >= b.age " +
                "AND e.since = 2013");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        properties.set("age", "25");
        TemporalVertex a = vertexFactory.createVertex("Person", properties);
        a.setTransactionTime(new Tuple2<>(123L, 1234L));
        a.setValidTime(new Tuple2<>(456L, 4567L));

        properties = Properties.create();
        properties.set("name", "Bob");
        properties.set("age", "24");
        TemporalVertex b = vertexFactory.createVertex("Person", properties);
        b.setTransactionTime(new Tuple2<>(1L, 2L));
        b.setValidTime(new Tuple2<>(3L, 4L));

        properties = Properties.create();
        properties.set("since", 2013);
        TemporalEdge e1 = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);
        e1.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e1.setValidTime(new Tuple2<>(12323L, 4123145325L));

        properties = Properties.create();
        properties.set("since", 2013);
        TemporalEdge e2 = edgeFactory.createEdge("knows", b.getId(), a.getId(), properties);
        e2.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e2.setValidTime(new Tuple2<>(12323L, 4123145325L));

        properties = Properties.create();
        properties.set("since", 2014);
        TemporalEdge e3 = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);
        e3.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e3.setValidTime(new Tuple2<>(12323L, 4123145325L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a, e1, b),
                new TripleTPGM(b, e2, a),
                new TripleTPGM(a, e3, b)
        );

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a.getId());
        assertEquals(result.get(0).getId(1), e1.getId());
        assertEquals(result.get(0).getId(2), b.getId());
    }

    @Test
    public void testFilterByLabel() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a:Person)-[e:likes]->(b)");

        TemporalVertex a1 = vertexFactory.createVertex("Person");
        TemporalVertex a2 = vertexFactory.createVertex("Orc");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e1 = edgeFactory.createEdge("likes", a1.getId(), b.getId());
        TemporalEdge e2 = edgeFactory.createEdge("likes", a2.getId(), b.getId());
        TemporalEdge e3 = edgeFactory.createEdge("knows", a1.getId(), b.getId());


        a1.setTransactionTime(new Tuple2<>(231L, 1231232L));
        a1.setValidTime(new Tuple2<>(123443623L, 4123145344345325L));
        a2.setTransactionTime(new Tuple2<>(231L, 1231232L));
        a2.setValidTime(new Tuple2<>(123443623L, 4123145344345325L));
        b.setTransactionTime(new Tuple2<>(231L, 1231232L));
        b.setValidTime(new Tuple2<>(123443623L, 4123145344345325L));
        e1.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e1.setValidTime(new Tuple2<>(12323L, 4123145325L));
        e2.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e2.setValidTime(new Tuple2<>(12323L, 4123145325L));
        e3.setTransactionTime(new Tuple2<>(12345L, 23234352L));
        e3.setValidTime(new Tuple2<>(12323L, 4123145325L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a1, e1, b),
                new TripleTPGM(a2, e2, b),
                new TripleTPGM(a1, e3, b)
        );


        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a1.getId());
        assertEquals(result.get(0).getId(1), e1.getId());
        assertEquals(result.get(0).getId(2), b.getId());
    }

    @Test
    public void testFilterByTime() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b) WHERE a.tx_from.before(b.tx_from)");

        TemporalVertex a1 = vertexFactory.createVertex("Person");
        TemporalVertex a2 = vertexFactory.createVertex("Orc");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e1 = edgeFactory.createEdge("likes", a1.getId(), b.getId());
        TemporalEdge e2 = edgeFactory.createEdge("likes", a2.getId(), b.getId());

        // a1 - b fulfills the predicate
        a1.setTransactionTime(new Tuple2<>(1L, 10L));
        a1.setValidTime(new Tuple2<>(1L, 10L));
        // a2 - b does not fulfill the predicate
        a2.setTransactionTime(new Tuple2<>(11L, 15L));
        a2.setValidTime(new Tuple2<>(11L, 15L));

        b.setTransactionTime(new Tuple2<>(8L, 20L));
        b.setValidTime(new Tuple2<>(8L, 20L));

        e1.setTransactionTime(new Tuple2<>(8L, 10L));
        e1.setValidTime(new Tuple2<>(8L, 10L));
        e2.setTransactionTime(new Tuple2<>(12L, 14L));
        e2.setValidTime(new Tuple2<>(12L, 14L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a1, e1, b),
                new TripleTPGM(a2, e2, b)
        );


        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a1.getId());
        assertEquals(result.get(0).getId(1), e1.getId());
        assertEquals(result.get(0).getId(2), b.getId());
    }

    @Test
    public void testFilterBySelfLoop() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(a)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
        TemporalEdge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(1L, 10L));
        b.setTransactionTime(new Tuple2<>(1L, 10L));
        b.setValidTime(new Tuple2<>(1L, 10L));
        e1.setTransactionTime(new Tuple2<>(1L, 10L));
        e1.setValidTime(new Tuple2<>(1L, 10L));
        e2.setTransactionTime(new Tuple2<>(1L, 10L));
        e2.setValidTime(new Tuple2<>(1L, 10L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a, e1, a),
                new TripleTPGM(a, e2, b)
        );

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "a",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a.getId());
        assertEquals(result.get(0).getId(1), e1.getId());
    }

    @Test
    public void testFilterByIsomorphism() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
        TemporalEdge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(1L, 10L));
        b.setTransactionTime(new Tuple2<>(1L, 10L));
        b.setValidTime(new Tuple2<>(1L, 10L));
        e1.setTransactionTime(new Tuple2<>(1L, 10L));
        e1.setValidTime(new Tuple2<>(1L, 10L));
        e2.setTransactionTime(new Tuple2<>(1L, 10L));
        e2.setValidTime(new Tuple2<>(1L, 10L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a, e1, a),
                new TripleTPGM(a, e2, b)
        );

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(result.get(0).getId(0), a.getId());
        assertEquals(result.get(0).getId(1), e2.getId());
        assertEquals(result.get(0).getId(2), b.getId());
    }

    @Test
    public void testFilterByHomomorphism() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
        TemporalEdge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(1L, 10L));
        b.setTransactionTime(new Tuple2<>(1L, 10L));
        b.setValidTime(new Tuple2<>(1L, 10L));
        e1.setTransactionTime(new Tuple2<>(1L, 10L));
        e1.setValidTime(new Tuple2<>(1L, 10L));
        e2.setTransactionTime(new Tuple2<>(1L, 10L));
        e2.setValidTime(new Tuple2<>(1L, 10L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(
                new TripleTPGM(a, e1, a),
                new TripleTPGM(a, e2, b)
        );

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.HOMOMORPHISM
        ).evaluate().collect();

        assertEquals(2, result.size());
    }

    @Test
    public void testResultingEntryList() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e = edgeFactory.createEdge("loves", a.getId(), b.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(123L, 1023L));
        b.setTransactionTime(new Tuple2<>(12L, 1230L));
        b.setValidTime(new Tuple2<>(156L, 10564L));
        e.setTransactionTime(new Tuple2<>(1456L, 145640L));
        e.setValidTime(new Tuple2<>(1456L, 105464L));


        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(new TripleTPGM(a, e, b));

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(3, result.get(0).size());
        assertEquals(a.getId(), result.get(0).getId(0));
        assertEquals(e.getId(), result.get(0).getId(1));
        assertEquals(b.getId(), result.get(0).getId(2));
    }

    @Test
    public void testResultingEntryForSelfLoops() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(a)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalEdge e = edgeFactory.createEdge("loves", a.getId(), a.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(123L, 1023L));
        e.setTransactionTime(new Tuple2<>(1456L, 145640L));
        e.setValidTime(new Tuple2<>(1456L, 105464L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(new TripleTPGM(a, e, a));

        List<EmbeddingTPGM> result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "a",
                predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
        ).evaluate().collect();

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
        assertEquals(a.getId(), result.get(0).getId(0));
        assertEquals(e.getId(), result.get(0).getId(1));
    }

    @Test
    public void testProjectionOfAvailableValues() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        Properties properties = Properties.create();
        properties.set("name", "Alice");
        properties.set("age", 25);
        TemporalVertex a = vertexFactory.createVertex("Person", properties);

        properties = Properties.create();
        properties.set("name", "Bob");
        properties.set("age", 24);
        TemporalVertex b = vertexFactory.createVertex("Person", properties);

        properties = Properties.create();
        properties.set("since", 2013);
        properties.set("active", true);
        TemporalEdge e = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(123L, 1023L));
        b.setTransactionTime(new Tuple2<>(12L, 1230L));
        b.setValidTime(new Tuple2<>(156L, 10564L));
        e.setTransactionTime(new Tuple2<>(1456L, 145640L));
        e.setValidTime(new Tuple2<>(1456L, 105464L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(new TripleTPGM(a, e, b));

        HashMap<String, List<String>> propertyKeys = new HashMap<>();
        propertyKeys.put("a", Lists.newArrayList("name", "age"));
        propertyKeys.put("e", Lists.newArrayList("since"));
        propertyKeys.put("b", Lists.newArrayList("name"));

        EmbeddingTPGM result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, propertyKeys, MatchStrategy.ISOMORPHISM
        ).evaluate().collect().get(0);

        assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
        assertEquals(result.getProperty(1), PropertyValue.create(25));
        assertEquals(result.getProperty(2), PropertyValue.create(2013));
        assertEquals(result.getProperty(3), PropertyValue.create("Bob"));
    }

    @Test
    public void testProjectionOfMissingValues() throws Exception {
        CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

        TemporalVertex a = vertexFactory.createVertex("Person");
        TemporalVertex b = vertexFactory.createVertex("Person");
        TemporalEdge e = edgeFactory.createEdge("knows", a.getId(), b.getId());

        a.setTransactionTime(new Tuple2<>(1L, 10L));
        a.setValidTime(new Tuple2<>(123L, 1023L));
        b.setTransactionTime(new Tuple2<>(12L, 1230L));
        b.setValidTime(new Tuple2<>(156L, 10564L));
        e.setTransactionTime(new Tuple2<>(1456L, 145640L));
        e.setValidTime(new Tuple2<>(1456L, 105464L));

        DataSet<TripleTPGM> triples = getExecutionEnvironment().fromElements(new TripleTPGM(a, e, b));

        HashMap<String, List<String>> propertyKeys = new HashMap<>();
        propertyKeys.put("a", Lists.newArrayList("name"));
        propertyKeys.put("e", Lists.newArrayList("since"));
        propertyKeys.put("b", Lists.newArrayList("name"));

        EmbeddingTPGM result = new FilterAndProjectTemporalTriples(triples,
                "a", "e", "b",
                predicates, propertyKeys, MatchStrategy.ISOMORPHISM
        ).evaluate().collect().get(0);

        assertEquals(result.getProperty(0), PropertyValue.NULL_VALUE);
        assertEquals(result.getProperty(1), PropertyValue.NULL_VALUE);
        assertEquals(result.getProperty(2), PropertyValue.NULL_VALUE);
    }
}
