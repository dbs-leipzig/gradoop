/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.util;

import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.junit.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import static org.junit.Assert.*;

public class AsciiGraphLoaderTest {

  private GradoopConfig<GraphHead, Vertex, Edge> config = 
    GradoopConfig.getDefaultConfig();

  @Test
  public void testFromString() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testFromFile() throws Exception {
    String file = URLDecoder.decode(
      getClass().getResource("/data/gdl/example.gdl").getFile(), 
      StandardCharsets.UTF_8.name());
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromFile(file, config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (GraphHead graphHead : asciiGraphLoader.getGraphHeads()) {
      assertEquals(
        "Graph has wrong label",
        GradoopConstants.DEFAULT_GRAPH_LABEL, graphHead.getLabel());
    }
  }

  @Test
  public void testGetGraphHeadByVariable() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()];h[()]", config);

    validateCollections(asciiGraphLoader, 2, 2, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    GraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");
    assertNotNull("graphHead was null", g);
    assertNotNull("graphHead was null", h);
    assertNotEquals("graphHeads were equal", g, h);
  }

  @Test
  public void testGetGraphHeadsByVariables() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()],h[()]", config);

    Collection<GraphHead> graphHeadPojos = asciiGraphLoader
      .getGraphHeadsByVariables("g", "h");

    assertEquals("Wrong number of graphs", 2, graphHeadPojos.size());
  }

  @Test
  public void testGetVertices() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()]", config);

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (Vertex vertex : asciiGraphLoader.getVertices()) {
      assertEquals("Vertex has wrong label",
        GradoopConstants.DEFAULT_VERTEX_LABEL, vertex.getLabel());
    }
  }

  @Test
  public void testGetVertexByVariable() {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("(a)", config);

    validateCollections(asciiGraphLoader, 0, 1, 0);
    validateCaches(asciiGraphLoader, 0, 1, 0);

    Vertex v = asciiGraphLoader.getVertexByVariable("a");
    assertEquals("Vertex has wrong label",
      GradoopConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
    assertNotNull("Vertex was null", v);
  }

  @Test
  public void testGetVerticesByVariables() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[(a),(b),(a)]", config);

    validateCollections(asciiGraphLoader, 1, 2, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Collection<Vertex> vertexs = asciiGraphLoader
      .getVerticesByVariables("a", "b");

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");

    assertEquals("Wrong number of vertices", 2, vertexs.size());
    assertTrue("Vertex was not contained in result", vertexs.contains(a));
    assertTrue("Vertex was not contained in result", vertexs.contains(b));
  }

  @Test
  public void testGetVerticesByGraphIds() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a),(b)],h[(a),(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    GraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<Vertex> vertexsG = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<Vertex> vertexsH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<Vertex> vertexsGH = asciiGraphLoader
      .getVerticesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, vertexsG.size());
    assertEquals("Wrong number of vertices", 2, vertexsH.size());
    assertEquals("Wrong number of vertices", 3, vertexsGH.size());
    assertTrue("Vertex was not contained in graph", vertexsG.contains(a));
    assertTrue("Vertex was not contained in graph", vertexsG.contains(b));
    assertTrue("Vertex was not contained in graph", vertexsH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexsH.contains(c));
    assertTrue("Vertex was not contained in graph", vertexsGH.contains(a));
    assertTrue("Vertex was not contained in graph", vertexsGH.contains(b));
    assertTrue("Vertex was not contained in graph", vertexsGH.contains(c));
  }

  @Test
  public void testGetVerticesByGraphVariables() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a),(b)],h[(a),(c)]", config);

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    Collection<Vertex> verticesG = asciiGraphLoader
      .getVerticesByGraphVariables("g");

    Collection<Vertex> verticesH = asciiGraphLoader
      .getVerticesByGraphVariables("h");

    Collection<Vertex> verticesGH = asciiGraphLoader
      .getVerticesByGraphVariables("g", "h");

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals("Wrong number of vertices", 2, verticesG.size());
    assertEquals("Wrong number of vertices", 2, verticesH.size());
    assertEquals("Wrong number of vertices", 3, verticesGH.size());
    assertTrue("Vertex was not contained in graph", verticesG.contains(a));
    assertTrue("Vertex was not contained in graph", verticesG.contains(b));
    assertTrue("Vertex was not contained in graph", verticesH.contains(a));
    assertTrue("Vertex was not contained in graph", verticesH.contains(c));
    assertTrue("Vertex was not contained in graph", verticesGH.contains(a));
    assertTrue("Vertex was not contained in graph", verticesGH.contains(b));
    assertTrue("Vertex was not contained in graph", verticesGH.contains(c));
  }

  @Test
  public void testGetEdges() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (Edge edge : asciiGraphLoader.getEdges()) {
      assertEquals("Edge has wrong label",
        GradoopConstants.DEFAULT_EDGE_LABEL, edge.getLabel());
    }
  }

  @Test
  public void testGetEdgesByVariables() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-[e]->()<-[f]-()]", config);

    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Collection<Edge> edges = asciiGraphLoader
      .getEdgesByVariables("e", "f");

    Edge e = asciiGraphLoader.getEdgeByVariable("e");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    assertEquals("Wrong number of edges", 2, edges.size());
    assertTrue("Edge was not contained in result", edges.contains(e));
    assertTrue("Edge was not contained in result", edges.contains(f));
  }

  @Test
  public void testGetEdgesByGraphIds() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()],h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    GraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    Collection<Edge> edgesG = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId()));

    Collection<Edge> edgesH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(h.getId()));

    Collection<Edge> edgesGH = asciiGraphLoader
      .getEdgesByGraphIds(GradoopIdSet.fromExisting(g.getId(), h.getId()));

    Edge a = asciiGraphLoader.getEdgeByVariable("a");
    Edge b = asciiGraphLoader.getEdgeByVariable("b");
    Edge c = asciiGraphLoader.getEdgeByVariable("c");
    Edge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgesG.size());
    assertEquals("Wrong number of edges", 2, edgesH.size());
    assertEquals("Wrong number of edges", 4, edgesGH.size());
    assertTrue("Edge was not contained in graph", edgesG.contains(a));
    assertTrue("Edge was not contained in graph", edgesG.contains(b));
    assertTrue("Edge was not contained in graph", edgesH.contains(c));
    assertTrue("Edge was not contained in graph", edgesH.contains(d));
    assertTrue("Edge was not contained in graph", edgesGH.contains(a));
    assertTrue("Edge was not contained in graph", edgesGH.contains(b));
    assertTrue("Edge was not contained in graph", edgesGH.contains(c));
    assertTrue("Edge was not contained in graph", edgesGH.contains(d));
  }

  @Test
  public void testGetEdgesByGraphVariables() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()],h[()-[c]->()-[d]->()]",
        config);

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    Collection<Edge> edgesG = asciiGraphLoader
      .getEdgesByGraphVariables("g");

    Collection<Edge> edgesH = asciiGraphLoader
      .getEdgesByGraphVariables("h");

    Collection<Edge> edgesGH = asciiGraphLoader
      .getEdgesByGraphVariables("g", "h");

    Edge a = asciiGraphLoader.getEdgeByVariable("a");
    Edge b = asciiGraphLoader.getEdgeByVariable("b");
    Edge c = asciiGraphLoader.getEdgeByVariable("c");
    Edge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals("Wrong number of edges", 2, edgesG.size());
    assertEquals("Wrong number of edges", 2, edgesH.size());
    assertEquals("Wrong number of edges", 4, edgesGH.size());
    assertTrue("Edge was not contained in graph", edgesG.contains(a));
    assertTrue("Edge was not contained in graph", edgesG.contains(b));
    assertTrue("Edge was not contained in graph", edgesH.contains(c));
    assertTrue("Edge was not contained in graph", edgesH.contains(d));
    assertTrue("Edge was not contained in graph", edgesGH.contains(a));
    assertTrue("Edge was not contained in graph", edgesGH.contains(b));
    assertTrue("Edge was not contained in graph", edgesGH.contains(c));
    assertTrue("Edge was not contained in graph", edgesGH.contains(d));
  }

  @Test
  public void testGetGraphHeadCache() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[()],h[()],[()]",
        config);

    validateCollections(asciiGraphLoader, 3, 3, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    GraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    GraphHead h = asciiGraphLoader.getGraphHeadByVariable("h");

    GraphHead gCache = asciiGraphLoader.getGraphHeadCache().get("g");
    GraphHead hCache = asciiGraphLoader.getGraphHeadCache().get("h");

    assertEquals("Graphs were not equal", g, gCache);
    assertEquals("Graphs were not equal", h, hCache);
  }

  @Test
  public void testGetVertexCache() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("(a),(b),()", config);

    validateCollections(asciiGraphLoader, 0, 3, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");

    Vertex aCache = asciiGraphLoader.getVertexCache().get("a");
    Vertex bCache = asciiGraphLoader.getVertexCache().get("b");

    assertEquals("Vertices were not equal", a, aCache);
    assertEquals("Vertices were not equal", b, bCache);
  }

  @Test
  public void testGetEdgeCache() throws Exception {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("()-[e]->()<-[f]-()-->()", config);

    validateCollections(asciiGraphLoader, 0, 4, 3);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Edge e = asciiGraphLoader.getEdgeByVariable("e");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    Edge eCache = asciiGraphLoader.getEdgeCache().get("e");
    Edge fCache = asciiGraphLoader.getEdgeCache().get("f");

    assertEquals("Edges were not equal", e, eCache);
    assertEquals("Edges were not equal", f, fCache);
  }

  @Test
  public void testAppendFromString() {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("[()-->()]");
    validateCollections(asciiGraphLoader, 2, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromString2() {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("[()-->()]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("()-->()");
    validateCollections(asciiGraphLoader, 1, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromStringWithVariables() {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g0[(a)-[e]->(b)]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g1[(a)-[e]->(b)]");
    validateCollections(asciiGraphLoader, 2, 2, 1);
    validateCaches(asciiGraphLoader, 2, 2, 1);

    GraphHead g1 = asciiGraphLoader.getGraphHeadByVariable("g0");
    GraphHead g2 = asciiGraphLoader.getGraphHeadByVariable("g1");
    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Edge e = asciiGraphLoader.getEdgeByVariable("e");

    assertEquals("Vertex has wrong graph count", 2, a.getGraphCount());
    assertTrue("Vertex was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("Vertex was not in g2", a.getGraphIds().contains(g2.getId()));

    assertEquals("Edge has wrong graph count", 2, e.getGraphCount());
    assertTrue("Edge was not in g1", a.getGraphIds().contains(g1.getId()));
    assertTrue("Edge was not in g2", a.getGraphIds().contains(g2.getId()));
  }

  @Test
  public void testUpdateFromStringWithVariables2() {
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader =
      AsciiGraphLoader.fromString("g[(a)-[e]->(b)]", config);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g[(a)-[f]->(c)]");
    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 1, 3, 2);

    GraphHead g = asciiGraphLoader.getGraphHeadByVariable("g");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    assertTrue("Vertex not in graph", c.getGraphIds().contains(g.getId()));
    assertTrue("Edge not in graph", f.getGraphIds().contains(g.getId()));
  }

  private void validateCollections(
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader,
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
    AsciiGraphLoader<GraphHead, Vertex, Edge> asciiGraphLoader,
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
