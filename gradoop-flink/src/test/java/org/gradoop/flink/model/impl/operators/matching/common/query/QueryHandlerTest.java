package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Lists;
import org.gradoop.flink.model.impl.operators.matching.common.query
  .QueryHandler;
import org.junit.Test;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.GDLHandler.Builder;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class QueryHandlerTest {

  static final String ECC_PROPERTY_KEY = "ecc";

  static final String TEST_QUERY = "" +
    "(v1:A {ecc=2})" +
    "(v2:B {ecc=1})" +
    "(v3:B {ecc=2})" +
    "(v1)-[e1:a]->(v2)" +
    "(v2)-[e2:b]->(v3)" +
    "(v2)-[e3:a]->(v1)" +
    "(v3)-[e4:c]->(v3)";

  static GDLHandler GDL_HANDLER = new Builder().buildFromString(TEST_QUERY);
  static QueryHandler QUERY_HANDLER = new QueryHandler(TEST_QUERY);

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

  private static <EL extends Element>
  boolean elementsEqual(List<EL> list, List<EL> expected) {
    boolean equal = list.size() == expected.size();

    if (equal) {
      Collections.sort(list, new ElementComparator<EL>());
      Collections.sort(expected, new ElementComparator<EL>());
      for (int i = 0; i < list.size(); i++) {
        if (!list.get(i).equals(expected.get(i))) {
          equal = false;
          break;
        }
      }
    }
    return equal;
  }

  private static class ElementComparator<EL extends Element>
    implements Comparator<EL> {
    @Override
    public int compare(EL o1, EL o2) {
      return Long.compare(o1.getId(), o2.getId());
    }
  }
}