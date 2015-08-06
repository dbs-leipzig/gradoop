//package org.gradoop;
//
//import com.google.common.collect.Lists;
//import org.gradoop.io.reader.EPGVertexReader;
//import org.gradoop.io.reader.SimpleVertexReader;
//import org.gradoop.io.reader.VertexLineReader;
//import org.gradoop.model.Attributed;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.VertexData;
//import org.gradoop.model.impl.EdgeFactory;
//import org.gradoop.storage.GraphStore;
//import org.junit.Rule;
//import org.junit.rules.TemporaryFolder;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.List;
//import java.util.Random;
//
//import static org.hamcrest.core.Is.is;
//import static org.junit.Assert.*;
//
///**
// * Root class for Gradoop tests. Contains sample graphs, corresponding
// * validation methods and helper methods.
// */
//public abstract class GradoopTest {
//
//  @Rule
//  public TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//  protected static final String KEY_1 = "k1";
//  protected static final String KEY_2 = "k2";
//  protected static final String KEY_3 = "k3";
//  protected static final String VALUE_1 = "v1";
//  protected static final String VALUE_2 = "v2";
//  protected static final String VALUE_3 = "v3";
//
//  private String getCallingMethod() {
//    return Thread.currentThread().getStackTrace()[1].getMethodName();
//  }
//
//  protected File getTempFile() throws IOException {
//    return getTempFile(null);
//  }
//
//  protected File getTempFile(String fileName) throws IOException {
//    fileName = (fileName != null) ? fileName :
//      getCallingMethod() + "_" + new Random().nextLong();
//    return temporaryFolder.newFile(fileName);
//  }
//
//  protected static final String[] BASIC_GRAPH =
//    new String[]{"0 1 2", "1 0 2", "2 1"};
//
//  protected static final String[] EXTENDED_GRAPH = new String[]{
//    "0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 2 k1 5 v1 k2 5 v2|1 0",
//    "1|B|2 k1 5 v1 k2 5 v2|b.0.0 2 k1 5 v1 k2 5 v2," +
//      "c.2.1 0|a.0.0 1 k1 5 v1|2 0 1",
//    "2|C|2 k1 5 v1 k2 5 v2|d.2.0 0|d.2.0 0,c.1.1 0|1 1" };
//
//  protected static final String[] EXTENDED_GRAPH_JSON = new String[]{
//    "{\"id\":0,\"label\":\"A\",\"properties\":{\"k1\":\"v1\", " +
//      "\"k2\":\"v2\", \"k3\":\"v3\"},\"out-edges\":[{\"otherid\":1," +
//      "\"label\":\"a\",\"properties\":{\"k1\": \"v1\"}}], " +
//      "\"in-edges\":[{\"otherid\":1,\"label\":\"b\"," +
//      "\"properties\":{\"k1\":\"v1\",\"k2\":\"v2\"}}],\"graphs\":[0]}",
//    "{\"id\":1,\"label\":\"B\",\"properties\":{\"k1\":\"v1\"," +
//      "\"k2\":\"v2\"},\"out-edges\":[{\"otherid\":0,\"label\":\"b\"," +
//      "\"properties\":{\"k1\":\"v1\",\"k2\":\"v2\"}},{\"otherid\":2," +
//      "\"label\":\"c\"}],\"in-edges\":[{\"otherid\":0,\"label\":\"a\"," +
//      "\"properties\":{\"k1\":\"v1\"}}],\"graphs\":[0, 1]}",
//    "{\"id\":2,\"label\":\"C\",\"properties\":{\"k1\":\"v1\"," +
//      "\"k2\":\"v2\"},\"out-edges\":[{\"otherid\":2,\"label\":\"d\"}]," +
//      "\"in-edges\":[{\"otherid\":	2,\"label\":\"d\"},{\"otherid\":1," +
//      "\"label\":\"c\"}],\"graphs\":[1]}" };
//
//  protected List<VertexData> createBasicGraphVertices() {
//    return createVertices(BASIC_GRAPH, new SimpleVertexReader());
//  }
//
//  protected List<VertexData> createExtendedGraphVertices() {
//    return createVertices(EXTENDED_GRAPH, new EPGVertexReader());
//  }
//
//  private List<VertexData> createVertices(String[] graph,
//    VertexLineReader vertexLineReader) {
//    List<VertexData> vertices = Lists.newArrayListWithCapacity(graph.length);
//    for (String line : graph) {
//      vertices.add(vertexLineReader.readVertex(line));
//    }
//    return vertices;
//  }
//
//  /**
//   * Validates if a given graph decoded as adjacency list matches the basic
//   * graph.
//   *
//   * @param textGraph graph under test
//   */
//  protected void validateBasicGraphVertices(String[] textGraph) {
//    List<VertexData> vertices = createVertices(textGraph, new SimpleVertexReader());
//    validateBasicGraphVertices(vertices);
//  }
//
//  protected void validateBasicGraphVertices(List<VertexData> vertices) {
//    assertEquals(3, vertices.size());
//    for (VertexData v : vertices) {
//      Long i = v.getId();
//      List<EdgeData> edgeDataList = Lists.newArrayList();
//      if (v.getOutgoingDegree() > 0) {
//        edgeDataList = Lists.newArrayList(v.getOutgoingEdges());
//      }
//      if (i.equals(0L)) {
//        validateBasicGraphEdges(edgeDataList, 2, 1, 2);
//      } else if (i.equals(1L)) {
//        validateBasicGraphEdges(edgeDataList, 2, 0, 2);
//      } else if (i.equals(2L)) {
//        validateBasicGraphEdges(edgeDataList, 1, 1);
//      }
//    }
//  }
//
//  private void validateBasicGraphEdges(List<EdgeData> edgeDataList, int expectedCount,
//    long... otherIDs) {
//    assertThat(edgeDataList.size(), is(expectedCount));
//    for (int i = 0; i < otherIDs.length; i++) {
//      boolean match = false;
//      for (EdgeData e : edgeDataList) {
//        if (e.getOtherID() == otherIDs[i]) {
//          match = true;
//          assertThat(e.getIndex(), is((long) i));
//        }
//      }
//      if (!match) {
//        assertTrue("edge list contains wrong edges", false);
//      }
//    }
//  }
//
//  protected void validateExtendedGraphVertices(GraphStore graphStore) {
//    List<VertexData> vertices =
//      Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
//    for (long id = 0; id < EXTENDED_GRAPH.length; id++) {
//      vertices.add(graphStore.readVertex(id));
//    }
//    validateExtendedGraphVertices(vertices);
//  }
//
//  protected void validateExtendedGraphEdges(List<EdgeData> result) {
//    assertEquals(4, result.size());
//    testEdge(result, 1L, "a", 0L, 1);
//    testEdge(result, 0L, "b", 0L, 2);
//    testEdge(result, 2L, "c", 1L, 0);
//    testEdge(result, 2L, "d", 0L, 0);
//  }
//
//  protected void validateExtendedGraphVertices(List<VertexData> result) {
//    assertEquals(EXTENDED_GRAPH.length, result.size());
//    for (VertexData v : result) {
//      System.out.println(v);
//      String label = v.getLabel();
//      List<Long> graphs = Lists.newArrayList(v.getGraphs());
//      List<EdgeData> outEdgeDatas = Lists.newArrayList(v.getOutgoingEdges());
//      List<EdgeData> inEdgeDatas = Lists.newArrayList(v.getIncomingEdges());
//
//      Long i = v.getId();
//      if (i.equals(0L)) {
//        // label (A)
//        assertEquals("A", label);
//        // properties (3 k1 5 v1 k2 5 v2 k3 5 v3)
//        testProperties(v, 3);
//        // out edges (a.1.0 1 k1 5 v1)
//        assertEquals(1, outEdgeDatas.size());
//        testEdge(outEdgeDatas, 1L, "a", 0L, 1);
//        // in edges (b.1.0 1 k1 5 v1)
//        assertEquals(1, inEdgeDatas.size());
//        testEdge(inEdgeDatas, 1L, "b", 0L, 2);
//        // graphs (1 0)
//        assertEquals(1, graphs.size());
//        assertTrue(graphs.contains(0L));
//      } else if (i.equals(1L)) {
//        // labels (B)
//        assertEquals("B", label);
//        // properties (2 k1 5 v1 k2 5 v2)
//        testProperties(v, 2);
//        // out edges (b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0)
//        assertEquals(2, outEdgeDatas.size());
//        testEdge(outEdgeDatas, 0L, "b", 0L, 2);
//        testEdge(outEdgeDatas, 2L, "c", 1L, 0);
//        // in edges (a.0.0 1 k1 5 v1)
//        assertEquals(1, inEdgeDatas.size());
//        testEdge(inEdgeDatas, 0L, "a", 0L, 1);
//        // graphs (2 0 1)
//        assertEquals(2, graphs.size());
//        assertTrue(graphs.contains(0L));
//        assertTrue(graphs.contains(1L));
//      } else if (i.equals(2L)) {
//        // labels (C)
//        assertEquals("C", label);
//        // properties (2 k1 5 v1 k2 5 v2)
//        testProperties(v, 2);
//        // out edges (d.2.0 0)
//        assertEquals(1, outEdgeDatas.size());
//        testEdge(outEdgeDatas, 2L, "d", 0L, 0);
//        // in edges (d.2.0 0,c.2.1 0)
//        assertEquals(2, inEdgeDatas.size());
//        testEdge(inEdgeDatas, 2L, "d", 0L, 0);
//        testEdge(inEdgeDatas, 1L, "c", 1L, 0);
//        // graphs (1 1)
//        assertEquals(1, graphs.size());
//        assertTrue(graphs.contains(1L));
//      } else {
//        assertTrue(false);
//      }
//    }
//  }
//
//  private void testEdge(List<EdgeData> edgeDatas, Long expectedOtherID,
//    String expectedLabel, Long expectedIndex, int expectedPropertyCount) {
//    EdgeData tmpEdgeData = EdgeFactory
//      .createDefaultEdgeWithLabel(expectedOtherID, expectedLabel,
//        expectedIndex);
//    assertTrue(edgeDatas.contains(tmpEdgeData));
//    int edgeIndex = edgeDatas.indexOf(tmpEdgeData);
//    testProperties(edgeDatas.get(edgeIndex), expectedPropertyCount);
//  }
//
//  private void testProperties(Attributed v, int expectedSize) {
//    int count = 0;
//    if (expectedSize == 0) {
//      assertNull(v.getPropertyKeys());
//    } else {
//      for (String propertyKey : v.getPropertyKeys()) {
//        switch (propertyKey) {
//        case KEY_1:
//          assertEquals(VALUE_1, v.getProperty(KEY_1));
//          break;
//        case KEY_2:
//          assertEquals(VALUE_2, v.getProperty(KEY_2));
//          break;
//        case KEY_3:
//          assertEquals(VALUE_3, v.getProperty(KEY_3));
//          break;
//        }
//        count++;
//      }
//      assertEquals(expectedSize, count);
//    }
//  }
//
//  /**
//   * Takes a given graph represented by a string array, writes it to a
//   * temporary file and returns a buffered reader which can be used for
//   * testing readers.
//   *
//   * @param graph string array representing lines in a file
//   * @return reader on the input graph
//   * @throws IOException
//   */
//  protected BufferedReader createTestReader(String[] graph) throws IOException {
//    File tmpFile = getTempFile();
//    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));
//
//    for (String line : graph) {
//      bw.write(line);
//      bw.newLine();
//    }
//    bw.flush();
//    bw.close();
//
//    return new BufferedReader(new FileReader(tmpFile));
//  }
//}
