package org.gradoop.util;

import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class AsciiGraphLoaderTest {

  private GradoopConfig<GraphHeadPojo, VertexPojo, EdgePojo> config =
    GradoopConfig.getDefaultConfig();

  @Test
  public void testFromString() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testFromFile() throws Exception {
    String file = getClass().getResource("/data/gdl/example.gdl").getFile();
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromFile(file, config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (GraphHeadPojo graphHeadPojo : asciiGraphLoader.getGraphHeads()) {
      assertEquals("Graph has wrong label",
        GConstants.DEFAULT_GRAPH_LABEL, graphHeadPojo.getLabel());
    }
  }

  @Test
  public void testGetGraphHeadByVariable() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()]", config);

    validateCollections(asciiGraphLoader, 2, 2, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    GraphHeadPojo g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHeadPojo h = asciiGraphLoader.getGraphHeadByVariable("h");
    assertNotNull("graphHead was null", g);
    assertNotNull("graphHead was null", h);
    assertNotEquals("graphHeads were equal", g, h);
  }

  @Test
  public void testGetGraphHeadsByVariables() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()]", config);

    Collection<GraphHeadPojo> graphHeadPojos = asciiGraphLoader
      .getGraphHeadsByVariables("g", "h");

    assertEquals("Wrong number of graphs", 2, graphHeadPojos.size());
  }

  @Test
  public void testGetVertices() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (VertexPojo vertexPojo : asciiGraphLoader.getVertices()) {
      assertEquals("Vertex has wrong label",
        GConstants.DEFAULT_VERTEX_LABEL, vertexPojo.getLabel());
    }
  }

  @Test
  public void testGetVertexByVariable() {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("(a)", config);

    validateCollections(asciiGraphLoader, 0, 1, 0);
    validateCaches(asciiGraphLoader, 0, 1, 0);

    VertexPojo v = asciiGraphLoader.getVertexByVariable("a");
    assertEquals("Vertex has wrong label",
      GConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
    assertNotNull("Vertex was null", v);
  }

  @Test
  public void testGetVerticesByVariables() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[(a);(b);(a)]", config);

    validateCollections(asciiGraphLoader, 1, 2, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Collection<VertexPojo> vertexPojos = asciiGraphLoader
      .getVerticesByVariables("a", "b");

    VertexPojo a = asciiGraphLoader.getVertexByVariable("a");
    VertexPojo b = asciiGraphLoader.getVertexByVariable("b");

    assertEquals("Wrong number of vertices", 2, vertexPojos.size());
    assertTrue("Vertex was not contained in result", vertexPojos.contains(a));
    assertTrue("Vertex was not contained in result", vertexPojos.contains(b));
  }

  @Test
  public void testGetVerticesByGraphIds() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a);(b)];h[(a);(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    GraphHeadPojo g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHeadPojo h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<VertexPojo> vertexPojosG = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<VertexPojo> vertexPojosH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<VertexPojo> vertexPojosGH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    VertexPojo a = asciiGraphLoader.getVertexByVariable("a");
    VertexPojo b = asciiGraphLoader.getVertexByVariable("b");
    VertexPojo c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, vertexPojosG.size());
    assertEquals("Wrong number of vertices", 2, vertexPojosH.size());
    assertEquals("Wrong number of vertices", 3, vertexPojosGH.size());
    assertTrue("Vertex was not contained in graph", vertexPojosG.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosG.contains(b));
    assertTrue("Vertex was not contained in graph", vertexPojosH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosH.contains(c));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(b));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(c));
  }

  @Test
  public void testGetVerticesByGraphVariables() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a);(b)];h[(a);(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    Collection<VertexPojo> vertexPojosG = asciiGraphLoader
      .getVerticesByGraphVariables("g");

    Collection<VertexPojo> vertexPojosH = asciiGraphLoader
      .getVerticesByGraphVariables("h");

    Collection<VertexPojo> vertexPojosGH = asciiGraphLoader
      .getVerticesByGraphVariables("g", "h");

    VertexPojo a = asciiGraphLoader.getVertexByVariable("a");
    VertexPojo b = asciiGraphLoader.getVertexByVariable("b");
    VertexPojo c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, vertexPojosG.size());
    assertEquals("Wrong number of vertices", 2, vertexPojosH.size());
    assertEquals("Wrong number of vertices", 3, vertexPojosGH.size());
    assertTrue("Vertex was not contained in graph", vertexPojosG.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosG.contains(b));
    assertTrue("Vertex was not contained in graph", vertexPojosH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosH.contains(c));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(b));
    assertTrue("Vertex was not contained in graph", vertexPojosGH.contains(c));
  }

  @Test
  public void testGetEdges() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (EdgePojo edgePojo : asciiGraphLoader.getEdges()) {
      assertEquals("Edge has wrong label",
        GConstants.DEFAULT_EDGE_LABEL, edgePojo.getLabel());
    }
  }

  @Test
  public void testGetEdgesByVariables() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-[e]->()<-[f]-()]", config);

    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Collection<EdgePojo> edgePojos = asciiGraphLoader
      .getEdgesByVariables("e", "f");

    EdgePojo e = asciiGraphLoader.getEdgeByVariable("e");
    EdgePojo f = asciiGraphLoader.getEdgeByVariable("f");

    assertEquals("Wrong number of edges", 2, edgePojos.size());
    assertTrue("Edge was not contained in result", edgePojos.contains(e));
    assertTrue("Edge was not contained in result", edgePojos.contains(f));
  }

  @Test
  public void testGetEdgesByGraphIds() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()];h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    GraphHeadPojo g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHeadPojo h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<EdgePojo> edgePojosG = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<EdgePojo> edgePojosH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<EdgePojo> edgePojosGH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    EdgePojo a = asciiGraphLoader.getEdgeByVariable("a");
    EdgePojo b = asciiGraphLoader.getEdgeByVariable("b");
    EdgePojo c = asciiGraphLoader.getEdgeByVariable("c");
    EdgePojo d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgePojosG.size());
    assertEquals("Wrong number of edges", 2, edgePojosH.size());
    assertEquals("Wrong number of edges", 4, edgePojosGH.size());
    assertTrue("Edge was not contained in graph", edgePojosG.contains(a));
    assertTrue("Edge was not contained in graph", edgePojosG.contains(b));
    assertTrue("Edge was not contained in graph", edgePojosH.contains(c));
    assertTrue("Edge was not contained in graph", edgePojosH.contains(d));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(a));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(b));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(c));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(d));
  }

  @Test
  public void testGetEdgesByGraphVariables() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()];h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    Collection<EdgePojo> edgePojosG = asciiGraphLoader
      .getEdgesByGraphVariables("g");

    Collection<EdgePojo> edgePojosH = asciiGraphLoader
      .getEdgesByGraphVariables("h");

    Collection<EdgePojo> edgePojosGH = asciiGraphLoader
      .getEdgesByGraphVariables("g", "h");

    EdgePojo a = asciiGraphLoader.getEdgeByVariable("a");
    EdgePojo b = asciiGraphLoader.getEdgeByVariable("b");
    EdgePojo c = asciiGraphLoader.getEdgeByVariable("c");
    EdgePojo d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgePojosG.size());
    assertEquals("Wrong number of edges", 2, edgePojosH.size());
    assertEquals("Wrong number of edges", 4, edgePojosGH.size());
    assertTrue("Edge was not contained in graph", edgePojosG.contains(a));
    assertTrue("Edge was not contained in graph", edgePojosG.contains(b));
    assertTrue("Edge was not contained in graph", edgePojosH.contains(c));
    assertTrue("Edge was not contained in graph", edgePojosH.contains(d));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(a));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(b));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(c));
    assertTrue("Edge was not contained in graph", edgePojosGH.contains(d));
  }

  @Test
  public void testGetGraphHeadCache() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()];[()]",
        config);

    validateCollections(asciiGraphLoader, 3, 3, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    GraphHeadPojo g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHeadPojo h = asciiGraphLoader.getGraphHeadByVariable("h");

    GraphHeadPojo gCache = asciiGraphLoader.getGraphHeadCache().get("g");
    GraphHeadPojo hCache = asciiGraphLoader.getGraphHeadCache().get("h");

    assertEquals("Graphs were not equal", g, gCache);
    assertEquals("Graphs were not equal", h, hCache);
  }

  @Test
  public void testGetVertexCache() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("(a);(b);()",
        config);

    validateCollections(asciiGraphLoader, 0, 3, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    VertexPojo a = asciiGraphLoader.getVertexByVariable("a");
    VertexPojo b = asciiGraphLoader.getVertexByVariable("b");

    VertexPojo aCache = asciiGraphLoader.getVertexCache().get("a");
    VertexPojo bCache = asciiGraphLoader.getVertexCache().get("b");

    assertEquals("Vertices were not equal", a, aCache);
    assertEquals("Vertices were not equal", b, bCache);
  }

  @Test
  public void testGetEdgeCache() throws Exception {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("()-[e]->()<-[f]-()-->()",
        config);

    validateCollections(asciiGraphLoader, 0, 4, 3);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    EdgePojo e = asciiGraphLoader.getEdgeByVariable("e");
    EdgePojo f = asciiGraphLoader.getEdgeByVariable("f");

    EdgePojo eCache = asciiGraphLoader.getEdgeCache().get("e");
    EdgePojo fCache = asciiGraphLoader.getEdgeCache().get("f");

    assertEquals("Edges were not equal", e, eCache);
    assertEquals("Edges were not equal", f, fCache);
  }

  @Test
  public void testAppendFromString() {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
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
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
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
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g0[(a)-[e]->(b)]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g1[(a)-[e]->(b)]");
    validateCollections(asciiGraphLoader, 2, 2, 1);
    validateCaches(asciiGraphLoader, 2, 2, 1);

    GraphHeadPojo g1 = asciiGraphLoader.getGraphHeadByVariable("g0");
    GraphHeadPojo g2 = asciiGraphLoader.getGraphHeadByVariable("g1");
    VertexPojo a = asciiGraphLoader.getVertexByVariable("a");
    EdgePojo e = asciiGraphLoader.getEdgeByVariable("e");

    assertEquals("Vertex has wrong graph count", 2, a.getGraphCount());
    assertTrue("Vertex was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("Vertex was not in g2", a.getGraphIds().contains(g2.getId()));

    assertEquals("Edge has wrong graph count", 2, e.getGraphCount());
    assertTrue("Edge was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("Edge was not in g2", a.getGraphIds().contains(g2.getId()));
  }

  @Test
  public void testUpdateFromStringWithVariables2() {
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a)-[e]->(b)]",
        config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g[(a)-[f]->(c)]");
    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 1, 3, 2);

    GraphHeadPojo g = asciiGraphLoader.getGraphHeadByVariable("g");
    VertexPojo c = asciiGraphLoader.getVertexByVariable("c");
    EdgePojo f = asciiGraphLoader.getEdgeByVariable("f");

    assertTrue("Vertex not in graph", c.getGraphIds().contains(g.getId()));
    assertTrue("Edge not in graph", f.getGraphIds().contains(g.getId()));
  }

  private void validateCollections(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader,
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
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> asciiGraphLoader,
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
