package org.gradoop.util;

import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.common.util.GConstants;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class AsciiGraphLoaderTest {

  private GradoopConfig config = GradoopConfig.getDefaultConfig();

  @Test
  public void testFromString() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testFromFile() throws Exception {
    String file = getClass().getResource("/data/gdl/example.gdl").getFile();
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromFile(file, config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (EPGMGraphHead graphHead : asciiGraphLoader.getGraphHeads()) {
      assertEquals("Graph has wrong label",
        GConstants.DEFAULT_GRAPH_LABEL, graphHead.getLabel());
    }
  }

  @Test
  public void testGetGraphHeadByVariable() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()]", config);

    validateCollections(asciiGraphLoader, 2, 2, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    EPGMGraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    EPGMGraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");
    assertNotNull("graphHead was null", g);
    assertNotNull("graphHead was null", h);
    assertNotEquals("graphHeads were equal", g, h);
  }

  @Test
  public void testGetGraphHeadsByVariables() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()]", config);

    Collection<EPGMGraphHead> graphHeadPojos = asciiGraphLoader
      .getGraphHeadsByVariables("g", "h");

    assertEquals("Wrong number of graphs", 2, graphHeadPojos.size());
  }

  @Test
  public void testGetVertices() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (EPGMVertex vertex : asciiGraphLoader.getVertices()) {
      assertEquals("EPGMVertex has wrong label",
        GConstants.DEFAULT_VERTEX_LABEL, vertex.getLabel());
    }
  }

  @Test
  public void testGetVertexByVariable() {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("(a)", config);

    validateCollections(asciiGraphLoader, 0, 1, 0);
    validateCaches(asciiGraphLoader, 0, 1, 0);

    EPGMVertex v = asciiGraphLoader.getVertexByVariable("a");
    assertEquals("EPGMVertex has wrong label",
      GConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
    assertNotNull("EPGMVertex was null", v);
  }

  @Test
  public void testGetVerticesByVariables() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[(a);(b);(a)]", config);

    validateCollections(asciiGraphLoader, 1, 2, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Collection<EPGMVertex> vertexs = asciiGraphLoader
      .getVerticesByVariables("a", "b");

    EPGMVertex a = asciiGraphLoader.getVertexByVariable("a");
    EPGMVertex b = asciiGraphLoader.getVertexByVariable("b");

    assertEquals("Wrong number of vertices", 2, vertexs.size());
    assertTrue("EPGMVertex was not contained in result", vertexs.contains(a));
    assertTrue("EPGMVertex was not contained in result", vertexs.contains(b));
  }

  @Test
  public void testGetVerticesByGraphIds() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a);(b)];h[(a);(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    EPGMGraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    EPGMGraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<EPGMVertex> vertexsG = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<EPGMVertex> vertexsH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<EPGMVertex> vertexsGH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    EPGMVertex a = asciiGraphLoader.getVertexByVariable("a");
    EPGMVertex b = asciiGraphLoader.getVertexByVariable("b");
    EPGMVertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, vertexsG.size());
    assertEquals("Wrong number of vertices", 2, vertexsH.size());
    assertEquals("Wrong number of vertices", 3, vertexsGH.size());
    assertTrue("EPGMVertex was not contained in graph", vertexsG.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsG.contains(b));
    assertTrue("EPGMVertex was not contained in graph", vertexsH.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsH.contains(c));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(b));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(c));
  }

  @Test
  public void testGetVerticesByGraphVariables() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a);(b)];h[(a);(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    Collection<EPGMVertex> vertexsG = asciiGraphLoader
      .getVerticesByGraphVariables("g");

    Collection<EPGMVertex> vertexsH = asciiGraphLoader
      .getVerticesByGraphVariables("h");

    Collection<EPGMVertex> vertexsGH = asciiGraphLoader
      .getVerticesByGraphVariables("g", "h");

    EPGMVertex a = asciiGraphLoader.getVertexByVariable("a");
    EPGMVertex b = asciiGraphLoader.getVertexByVariable("b");
    EPGMVertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, vertexsG.size());
    assertEquals("Wrong number of vertices", 2, vertexsH.size());
    assertEquals("Wrong number of vertices", 3, vertexsGH.size());
    assertTrue("EPGMVertex was not contained in graph", vertexsG.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsG.contains(b));
    assertTrue("EPGMVertex was not contained in graph", vertexsH.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsH.contains(c));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(a));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(b));
    assertTrue("EPGMVertex was not contained in graph", vertexsGH.contains(c));
  }

  @Test
  public void testGetEdges() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (EPGMEdge edge : asciiGraphLoader.getEdges()) {
      assertEquals("EPGMEdge has wrong label",
        GConstants.DEFAULT_EDGE_LABEL, edge.getLabel());
    }
  }

  @Test
  public void testGetEdgesByVariables() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-[e]->()<-[f]-()]", config);

    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Collection<EPGMEdge> edges = asciiGraphLoader
      .getEdgesByVariables("e", "f");

    EPGMEdge e = asciiGraphLoader.getEdgeByVariable("e");
    EPGMEdge f = asciiGraphLoader.getEdgeByVariable("f");

    assertEquals("Wrong number of edges", 2, edges.size());
    assertTrue("EPGMEdge was not contained in result", edges.contains(e));
    assertTrue("EPGMEdge was not contained in result", edges.contains(f));
  }

  @Test
  public void testGetEdgesByGraphIds() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()];h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    EPGMGraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    EPGMGraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<EPGMEdge> edgesG = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<EPGMEdge> edgesH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<EPGMEdge> edgesGH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    EPGMEdge a = asciiGraphLoader.getEdgeByVariable("a");
    EPGMEdge b = asciiGraphLoader.getEdgeByVariable("b");
    EPGMEdge c = asciiGraphLoader.getEdgeByVariable("c");
    EPGMEdge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgesG.size());
    assertEquals("Wrong number of edges", 2, edgesH.size());
    assertEquals("Wrong number of edges", 4, edgesGH.size());
    assertTrue("EPGMEdge was not contained in graph", edgesG.contains(a));
    assertTrue("EPGMEdge was not contained in graph", edgesG.contains(b));
    assertTrue("EPGMEdge was not contained in graph", edgesH.contains(c));
    assertTrue("EPGMEdge was not contained in graph", edgesH.contains(d));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(a));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(b));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(c));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(d));
  }

  @Test
  public void testGetEdgesByGraphVariables() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()];h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    Collection<EPGMEdge> edgesG = asciiGraphLoader
      .getEdgesByGraphVariables("g");

    Collection<EPGMEdge> edgesH = asciiGraphLoader
      .getEdgesByGraphVariables("h");

    Collection<EPGMEdge> edgesGH = asciiGraphLoader
      .getEdgesByGraphVariables("g", "h");

    EPGMEdge a = asciiGraphLoader.getEdgeByVariable("a");
    EPGMEdge b = asciiGraphLoader.getEdgeByVariable("b");
    EPGMEdge c = asciiGraphLoader.getEdgeByVariable("c");
    EPGMEdge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgesG.size());
    assertEquals("Wrong number of edges", 2, edgesH.size());
    assertEquals("Wrong number of edges", 4, edgesGH.size());
    assertTrue("EPGMEdge was not contained in graph", edgesG.contains(a));
    assertTrue("EPGMEdge was not contained in graph", edgesG.contains(b));
    assertTrue("EPGMEdge was not contained in graph", edgesH.contains(c));
    assertTrue("EPGMEdge was not contained in graph", edgesH.contains(d));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(a));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(b));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(c));
    assertTrue("EPGMEdge was not contained in graph", edgesGH.contains(d));
  }

  @Test
  public void testGetGraphHeadCache() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()];[()]",
        config);

    validateCollections(asciiGraphLoader, 3, 3, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    EPGMGraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    EPGMGraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    EPGMGraphHead gCache = asciiGraphLoader.getGraphHeadCache().get("g");
    EPGMGraphHead hCache = asciiGraphLoader.getGraphHeadCache().get("h");

    assertEquals("Graphs were not equal", g, gCache);
    assertEquals("Graphs were not equal", h, hCache);
  }

  @Test
  public void testGetVertexCache() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("(a);(b);()",
        config);

    validateCollections(asciiGraphLoader, 0, 3, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    EPGMVertex a = asciiGraphLoader.getVertexByVariable("a");
    EPGMVertex b = asciiGraphLoader.getVertexByVariable("b");

    EPGMVertex aCache = asciiGraphLoader.getVertexCache().get("a");
    EPGMVertex bCache = asciiGraphLoader.getVertexCache().get("b");

    assertEquals("Vertices were not equal", a, aCache);
    assertEquals("Vertices were not equal", b, bCache);
  }

  @Test
  public void testGetEdgeCache() throws Exception {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("()-[e]->()<-[f]-()-->()",
        config);

    validateCollections(asciiGraphLoader, 0, 4, 3);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    EPGMEdge e = asciiGraphLoader.getEdgeByVariable("e");
    EPGMEdge f = asciiGraphLoader.getEdgeByVariable("f");

    EPGMEdge eCache = asciiGraphLoader.getEdgeCache().get("e");
    EPGMEdge fCache = asciiGraphLoader.getEdgeCache().get("f");

    assertEquals("Edges were not equal", e, eCache);
    assertEquals("Edges were not equal", f, fCache);
  }

  @Test
  public void testAppendFromString() {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("[()-->()]");
    validateCollections(asciiGraphLoader, 2, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromString2() {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("()-->()");
    validateCollections(asciiGraphLoader, 1, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromStringWithVariables() {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g0[(a)-[e]->(b)]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g1[(a)-[e]->(b)]");
    validateCollections(asciiGraphLoader, 2, 2, 1);
    validateCaches(asciiGraphLoader, 2, 2, 1);

    EPGMGraphHead g1 = asciiGraphLoader.getGraphHeadByVariable("g0");
    EPGMGraphHead g2 = asciiGraphLoader.getGraphHeadByVariable("g1");
    EPGMVertex a = asciiGraphLoader.getVertexByVariable("a");
    EPGMEdge e = asciiGraphLoader.getEdgeByVariable("e");

    assertEquals("EPGMVertex has wrong graph count", 2, a.getGraphCount());
    assertTrue("EPGMVertex was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("EPGMVertex was not in g2", a.getGraphIds().contains(g2.getId()));

    assertEquals("EPGMEdge has wrong graph count", 2, e.getGraphCount());
    assertTrue("EPGMEdge was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("EPGMEdge was not in g2", a.getGraphIds().contains(g2.getId()));
  }

  @Test
  public void testUpdateFromStringWithVariables2() {
    AsciiGraphLoader asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a)-[e]->(b)]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g[(a)-[f]->(c)]");
    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 1, 3, 2);

    EPGMGraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    EPGMVertex c = asciiGraphLoader.getVertexByVariable("c");
    EPGMEdge f = asciiGraphLoader.getEdgeByVariable("f");

    assertTrue("EPGMVertex not in graph", c.getGraphIds().contains(g.getId()));
    assertTrue("EPGMEdge not in graph", f.getGraphIds().contains(g.getId()));
  }

  private void validateCollections(
    AsciiGraphLoader asciiGraphLoader,
    int expectedGraphHeadCount,
    int expectedVertexCount,
    int expectedEdgeCount) {
    assertEquals("wrong graph head count", expectedGraphHeadCount,
      asciiGraphLoader.getGraphHeads().size());
    assertEquals("wrong vertex count", expectedVertexCount,
      asciiGraphLoader.getVertices().size());
    assertEquals("wrong edge count", expectedEdgeCount,
      asciiGraphLoader.getEdges().size());
  }

  private void validateCaches(
    AsciiGraphLoader asciiGraphLoader,
    int expectedGraphHeadCacheCount,
    int expectedVertexCacheCount,
    int expectedEdgeCacheCount) {
    assertEquals("wrong graph head cache count", expectedGraphHeadCacheCount,
      asciiGraphLoader.getGraphHeadCache().size());
    assertEquals("wrong vertex cache count", expectedVertexCacheCount,
      asciiGraphLoader.getVertexCache().size());
    assertEquals("wrong edge cache count", expectedEdgeCacheCount,
      asciiGraphLoader.getEdgeCache().size());
  }
}
