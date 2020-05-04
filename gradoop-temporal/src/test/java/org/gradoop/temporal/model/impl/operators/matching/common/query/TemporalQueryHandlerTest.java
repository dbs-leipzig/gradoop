package org.gradoop.temporal.model.impl.operators.matching.common.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.Triple;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util.QueryPredicateFactory;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.Collection;
import java.util.List;
import java.util.Set;


import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;


public class TemporalQueryHandlerTest {

    @Test
    public void testGetPredicatesWithoutDefaultAsOf(){
        String testquery = "MATCH (v1)-[e1:test]->(v2) WHERE v1.tx_from.before(v2.tx_to)";
        TemporalQueryHandler handler = new TemporalQueryHandler(testquery);

        Predicate and = new And(
                new Comparison(new TimeSelector("v2", "tx_to"),
                        Comparator.GT,
                        new TimeSelector("v1", "tx_from")),
                new Comparison(new PropertySelector("e1", "__label__"),
                        Comparator.EQ,
                        new Literal("test"))
        );
        assertPredicateEquals(and, handler.getPredicates());
    }

    @Test
    public void testGetPredicatesWithDefaultAsOf(){
        String testquery = "MATCH (v1)-[e1:test]->(v2) WHERE v1.tx_from.before(v2.val_to)";
        TemporalQueryHandler handler = new TemporalQueryHandler(testquery);
        TimeLiteral now = handler.getNow();
        Predicate and = new And(
                new Comparison(new TimeSelector("v2", "val_to"),
                        Comparator.GT,
                        new TimeSelector("v1", "tx_from")),
                new Comparison(new PropertySelector("e1", "__label__"),
                        Comparator.EQ,
                        new Literal("test"))
        );
        And defaultAsOfV1 = getAsOf("v1", now);
        And defaultAsOfE1 = getAsOf("e1", now);
        And defaultAsOfV2 = getAsOf("v2", now);

        Predicate expectedGDLPredicate = new And(and, defaultAsOfE1);
        expectedGDLPredicate = new And(expectedGDLPredicate, defaultAsOfV2);
        expectedGDLPredicate = new And(expectedGDLPredicate, defaultAsOfV1);

        assertPredicateEquals(expectedGDLPredicate, handler.getPredicates());

        //without any other predicates
        testquery = "MATCH (v1)-[e1]->(v2)";
        handler = new TemporalQueryHandler(testquery);
        now = handler.getNow();
        defaultAsOfV1 = getAsOf("v1", now);
        defaultAsOfE1 = getAsOf("e1", now);
        defaultAsOfV2 = getAsOf("v2", now);
        expectedGDLPredicate = new And(defaultAsOfE1, defaultAsOfV2);
        expectedGDLPredicate = new And(expectedGDLPredicate, defaultAsOfV1);
        assertPredicateEquals(expectedGDLPredicate, handler.getPredicates());

    }

    private void assertPredicateEquals(Predicate expectedGDL, CNF result){
        CNF expected = QueryPredicateFactory.createFrom(expectedGDL).asCNF();
        equalCNFs(expected, result);
    }

    private void equalCNFs(CNF a, CNF b){
        assertEquals(a.getPredicates().size(), b.getPredicates().size());
        for(CNFElement aElement: a.getPredicates()){
            ComparisonExpression aComp = aElement.getPredicates().get(0);
            boolean foundA = false;
            for(CNFElement bElement: b.getPredicates()){
                ComparisonExpression bComp = bElement.getPredicates().get(0);
                if(comparisonEqual(aComp, bComp)){
                    foundA= true;
                }
            }
            assertTrue(foundA);
        }
    }

    private boolean comparisonEqual(ComparisonExpression a, ComparisonExpression b){
        Comparison aComp = ((ComparisonExpressionTPGM)a).getGDLComparison();
        Comparison bComp = ((ComparisonExpressionTPGM)b).getGDLComparison();
        return (aComp.equals(bComp)||aComp.switchSides().equals(bComp));
    }


    private And getAsOf(String var, TimeLiteral now){
        return new And(
                new Comparison(
                        new TimeSelector(var, TimeSelector.TimeField.TX_FROM),
                        Comparator.LTE, now
                ),
                new Comparison(
                        new TimeSelector(var, TimeSelector.TimeField.TX_TO),
                        Comparator.GTE, now
                )
        );
    }


    /**
     * copied from QueryHandlerTest (changed to TemporalQueryHandler). Ensure that this still works
     */
    static final String ECC_PROPERTY_KEY = "ecc";

    static final String TEST_QUERY = "" +
            "(v1:A {ecc : 2})" +
            "(v2:B {ecc : 1})" +
            "(v3:B {ecc : 2})" +
            "(v1)-[e1:a]->(v2)" +
            "(v2)-[e2:b]->(v3)" +
            "(v2)-[e3:a]->(v1)" +
            "(v3)-[e4:c]->(v3)";

    private static GDLHandler GDL_HANDLER = new GDLHandler.Builder().buildFromString(TEST_QUERY);
    static TemporalQueryHandler QUERY_HANDLER = new TemporalQueryHandler(TEST_QUERY);

    @Test
    public void testGetTriples() throws Exception {
        Set<Triple> expected = Sets.newHashSet(
                new Triple(GDL_HANDLER.getVertexCache().get("v1"), GDL_HANDLER.getEdgeCache().get("e1"),
                        GDL_HANDLER.getVertexCache().get("v2")),
                new Triple(GDL_HANDLER.getVertexCache().get("v2"), GDL_HANDLER.getEdgeCache().get("e2"),
                        GDL_HANDLER.getVertexCache().get("v3")),
                new Triple(GDL_HANDLER.getVertexCache().get("v2"), GDL_HANDLER.getEdgeCache().get("e3"),
                        GDL_HANDLER.getVertexCache().get("v1")),
                new Triple(GDL_HANDLER.getVertexCache().get("v3"), GDL_HANDLER.getEdgeCache().get("e4"),
                        GDL_HANDLER.getVertexCache().get("v3")));

        Collection<Triple> triples = QUERY_HANDLER.getTriples();
        assertEquals(expected.size(), triples.size());
        assertTrue(expected.containsAll(triples));
    }

    @Test
    public void testGetVertexCount() {
        assertEquals(3, QUERY_HANDLER.getVertexCount());
    }

    @Test
    public void testGetEdgeCount() {
        assertEquals(4, QUERY_HANDLER.getEdgeCount());
    }

    @Test
    public void testIsSingleVertexGraph() {
        assertFalse(QUERY_HANDLER.isSingleVertexGraph());
        assertTrue(new QueryHandler("(v0)").isSingleVertexGraph());
    }

    @Test
    public void testGetDiameter() {
        assertEquals(2, QUERY_HANDLER.getDiameter());
        assertEquals(0, new QueryHandler("(v0)").getDiameter());
    }

    @Test
    public void testGetRadius() {
        assertEquals(1, QUERY_HANDLER.getRadius());
        assertEquals(0, new QueryHandler("(v0)").getRadius());
    }

    @Test
    public void testIsVertex() {
        assertTrue(QUERY_HANDLER.isVertex("v1"));
        assertFalse(QUERY_HANDLER.isVertex("e1"));
    }

    @Test
    public void testIsEdge() {
        assertTrue(QUERY_HANDLER.isEdge("e1"));
        assertFalse(QUERY_HANDLER.isEdge("v1"));
    }

    @Test
    public void testGetVertexById() throws Exception {
        Vertex expected = GDL_HANDLER.getVertexCache().get("v1");
        assertTrue(QUERY_HANDLER.getVertexById(expected.getId()).equals(expected));
    }

    @Test
    public void testGetEdgeById() throws Exception {
        Edge expected = GDL_HANDLER.getEdgeCache().get("e1");
        assertTrue(QUERY_HANDLER.getEdgeById(expected.getId()).equals(expected));
    }

    @Test
    public void testGetVertexByVariable() throws Exception {
        Vertex expected = GDL_HANDLER.getVertexCache().get("v1");
        assertEquals(QUERY_HANDLER.getVertexByVariable("v1"), expected);
        assertNotEquals(QUERY_HANDLER.getVertexByVariable("v2"), expected);
    }

    @Test
    public void testGetEdgeByVariable() throws Exception {
        Edge expected = GDL_HANDLER.getEdgeCache().get("e1");
        assertEquals(QUERY_HANDLER.getEdgeByVariable("e1"), expected);
        assertNotEquals(QUERY_HANDLER.getEdgeByVariable("e2"), expected);
    }

    @Test
    public void testGetVerticesByLabel() throws Exception {
        List<Vertex> bVertices = Lists.newArrayList(
                QUERY_HANDLER.getVerticesByLabel("B"));
        List<Vertex> expected = Lists.newArrayList(
                GDL_HANDLER.getVertexCache().get("v2"),
                GDL_HANDLER.getVertexCache().get("v3"));
        assertTrue(elementsEqual(bVertices, expected));
    }

    @Test
    public void testGetNeighbors() throws Exception {
        List<Vertex> neighbors = Lists.newArrayList(QUERY_HANDLER
                .getNeighbors(GDL_HANDLER.getVertexCache().get("v2").getId()));
        List<Vertex> expected = Lists.newArrayList(
                GDL_HANDLER.getVertexCache().get("v1"),
                GDL_HANDLER.getVertexCache().get("v3"));
        assertTrue(elementsEqual(neighbors, expected));
    }

    @Test
    public void testGetEdgesByLabel() throws Exception {
        List<Edge> aEdges = Lists.newArrayList(QUERY_HANDLER.getEdgesByLabel("a"));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e1"),
                GDL_HANDLER.getEdgeCache().get("e3"));
        assertTrue(elementsEqual(aEdges, expected));
    }

    @Test
    public void testGetEdgesByVertexId() throws Exception {
        Vertex v2 = GDL_HANDLER.getVertexCache().get("v2");
        List<Edge> edges = Lists.newArrayList(
                QUERY_HANDLER.getEdgesByVertexId(v2.getId()));

        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e1"),
                GDL_HANDLER.getEdgeCache().get("e2"),
                GDL_HANDLER.getEdgeCache().get("e3"));
        assertTrue(elementsEqual(edges, expected));
    }

    @Test
    public void testGetEdgesBySourceVertexId() throws Exception {
        Vertex v2 = GDL_HANDLER.getVertexCache().get("v2");
        List<Edge> outE = Lists.newArrayList(
                QUERY_HANDLER.getEdgesBySourceVertexId(v2.getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e2"),
                GDL_HANDLER.getEdgeCache().get("e3"));
        assertTrue(elementsEqual(outE, expected));
    }

    @Test
    public void testGetEdgesByTargetVertexId() throws Exception {
        Vertex v3 = GDL_HANDLER.getVertexCache().get("v3");
        List<Edge> inE = Lists.newArrayList(
                QUERY_HANDLER.getEdgesByTargetVertexId(v3.getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e2"),
                GDL_HANDLER.getEdgeCache().get("e4"));
        assertTrue(elementsEqual(inE, expected));
    }

    @Test
    public void testGetPredecessors() throws Exception {
        List<Edge> predecessors = Lists.newArrayList(QUERY_HANDLER.getPredecessors(
                GDL_HANDLER.getEdgeCache().get("e2").getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e1"));
        assertTrue(elementsEqual(predecessors, expected));
    }

    @Test
    public void testGetPredecessorsWithLoop() throws Exception {
        List<Edge> predecessors = Lists.newArrayList(QUERY_HANDLER.getPredecessors(
                GDL_HANDLER.getEdgeCache().get("e4").getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e2"),
                GDL_HANDLER.getEdgeCache().get("e4"));
        assertTrue(elementsEqual(predecessors, expected));
    }

    @Test
    public void testGetSuccessors() throws Exception {
        List<Edge> successors = Lists.newArrayList(QUERY_HANDLER.getSuccessors(
                GDL_HANDLER.getEdgeCache().get("e1").getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e2"),
                GDL_HANDLER.getEdgeCache().get("e3"));
        assertTrue(elementsEqual(successors, expected));
    }

    @Test
    public void testGetSuccessorsWithLoop() throws Exception {
        List<Edge> successors = Lists.newArrayList(QUERY_HANDLER.getSuccessors(
                GDL_HANDLER.getEdgeCache().get("e4").getId()));
        List<Edge> expected = Lists.newArrayList(
                GDL_HANDLER.getEdgeCache().get("e4"));
        assertTrue(elementsEqual(successors, expected));
    }

    @Test
    public void testGetCenterVertices() throws Exception {
        List<Vertex> centerVertices = Lists.newArrayList(
                QUERY_HANDLER.getCenterVertices());
        List<Vertex> expected = Lists.newArrayList(
                GDL_HANDLER.getVertexCache().get("v2"));
        assertTrue(elementsEqual(centerVertices, expected));
    }

    private static <EL extends Element> boolean elementsEqual(List<EL> list, List<EL> expected) {
        boolean equal = list.size() == expected.size();

        if (equal) {
            list.sort(java.util.Comparator.comparingLong(Element::getId));
            expected.sort(java.util.Comparator.comparingLong(Element::getId));
            for (int i = 0; i < list.size(); i++) {
                if (!list.get(i).equals(expected.get(i))) {
                    equal = false;
                    break;
                }
            }
        }
        return equal;
    }
}
