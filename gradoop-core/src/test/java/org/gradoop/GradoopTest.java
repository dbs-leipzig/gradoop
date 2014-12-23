package org.gradoop;

import com.google.common.collect.Lists;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.io.reader.SimpleVertexReader;
import org.gradoop.io.reader.VertexLineReader;
import org.gradoop.model.Attributed;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.EdgeFactory;
import org.gradoop.storage.GraphStore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Default Test class for gradoop. Contains a few graphs and some helper
 * methods.
 */
public abstract class GradoopTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected static final String KEY_1 = "k1";
  protected static final String KEY_2 = "k2";
  protected static final String KEY_3 = "k3";
  protected static final String VALUE_1 = "v1";
  protected static final String VALUE_2 = "v2";
  protected static final String VALUE_3 = "v3";

  private String getCallingMethod() {
    return Thread.currentThread().getStackTrace()[1].getMethodName();
  }

  protected File getTempFile()
    throws IOException {
    return getTempFile(null);
  }

  protected File getTempFile(String fileName)
    throws IOException {
    fileName = (fileName != null) ? fileName :
      getCallingMethod() + "_" + new Random().nextLong();
    return temporaryFolder.newFile(fileName);
  }

  protected static final String[] BASIC_GRAPH = new String[] {
    "0 1 2",
    "1 0 2",
    "2 1"
  };

  protected static final String[] EXTENDED_GRAPH = new String[] {
    "0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 1 k1 5 v1|1 0",
    "1|A B|2 k1 5 v1 k2 5 v2|b.0.0 2 k1 5 v1 k2 5 v2," +
      "c.2.1 0|a.0.0 1 k1 5 v1|2 0 1",
    "2|C|2 k1 5 v1 k2 5 v2|d.2.0 0|d.2.0 0,c.2.1 0|1 1"
  };

  protected List<Vertex> createBasicGraphVertices() {
    return createVertices(BASIC_GRAPH, new SimpleVertexReader());
  }

  protected List<Vertex> createExtendedGraphVertices() {
    return createVertices(EXTENDED_GRAPH, new EPGVertexReader());
  }

  private List<Vertex> createVertices(String[] graph,
                                      VertexLineReader vertexLineReader) {
    List<Vertex> vertices = Lists.newArrayListWithCapacity(graph.length);
    for (String line : graph) {
      vertices.add(vertexLineReader.readVertex(line));
    }
    return vertices;
  }

  protected void validateBasicGraphVertices(List<Vertex> vertices) {
    assertEquals(3, vertices.size());
    for (Vertex v : vertices) {
      Long i = v.getID();
      if (i.equals(0L)) {
        validateBasicGraphEdges(v.getOutgoingEdges(), 2, 1, 2);
      } else if (i.equals(1L)) {
        validateBasicGraphEdges(v.getOutgoingEdges(), 2, 0, 2);
      } else if (i.equals(2L)) {
        validateBasicGraphEdges(v.getOutgoingEdges(), 1, 1);
      }
    }
  }

  private void validateBasicGraphEdges(Iterable<Edge> edges, int expectedCount,
                                       long... otherIDs) {
    List<Edge> edgeList = Lists.newArrayList(edges);
    assertThat(edgeList.size(), is(expectedCount));
    for (int i = 0; i < otherIDs.length; i++) {
      boolean match = false;
      for (Edge e : edgeList) {
        if (e.getOtherID() == otherIDs[i]) {
          match = true;
          assertThat(e.getIndex(), is((long) i));
        }
      }
      if (!match) {
        assertTrue("edge list contains wrong edges", false);
      }
    }
  }

  protected void validateExtendedGraphVertices(GraphStore graphStore) {
    List<Vertex> vertices = Lists.newArrayListWithCapacity(EXTENDED_GRAPH
      .length);
    for (long id = 0; id < EXTENDED_GRAPH.length; id++) {
      vertices.add(graphStore.readVertex(id));
    }
    validateExtendedGraphVertices(vertices);
  }

  protected void validateExtendedGraphVertices(List<Vertex> result) {
    assertEquals(EXTENDED_GRAPH.length, result.size());
    for (Vertex v : result) {
      List<String> labels = Lists.newArrayList(v.getLabels());
      List<Long> graphs = Lists.newArrayList(v.getGraphs());
      List<Edge> outEdges = Lists.newArrayList(v.getOutgoingEdges());
      List<Edge> inEdges = Lists.newArrayList(v.getIncomingEdges());

      Long i = v.getID();
      if (i.equals(0L)) {
        // labels (A)
        assertEquals(1, labels.size());
        assertTrue(labels.contains("A"));
        // properties (3 k1 5 v1 k2 5 v2 k3 5 v3)
        testProperties(v, 3);
        // out edges (a.1.0 1 k1 5 v1)
        assertEquals(1, outEdges.size());
        testEdge(outEdges, 1L, "a", 0L, 1);
        // in edges (b.1.0 1 k1 5 v1)
        assertEquals(1, inEdges.size());
        testEdge(inEdges, 1L, "b", 0L, 1);
        // graphs (1 0)
        assertEquals(1, graphs.size());
        assertTrue(graphs.contains(0L));
      } else if (i.equals(1L)) {
        // labels (A,B)
        assertEquals(2, labels.size());
        assertTrue(labels.contains("A"));
        assertTrue(labels.contains("B"));
        // properties (2 k1 5 v1 k2 5 v2)
        testProperties(v, 2);
        // out edges (b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0)
        assertEquals(2, outEdges.size());
        testEdge(outEdges, 0L, "b", 0L, 2);
        testEdge(outEdges, 2L, "c", 1L, 0);
        // in edges (a.0.0 1 k1 5 v1)
        assertEquals(1, inEdges.size());
        testEdge(inEdges, 0L, "a", 0L, 1);
        // graphs (2 0 1)
        assertEquals(2, graphs.size());
        assertTrue(graphs.contains(0L));
        assertTrue(graphs.contains(1L));
      } else if (i.equals(2L)) {
        // labels (C)
        assertEquals(1, labels.size());
        assertTrue(labels.contains("C"));
        // properties (2 k1 5 v1 k2 5 v2)
        testProperties(v, 2);
        // out edges (d.2.0 0)
        assertEquals(1, outEdges.size());
        testEdge(outEdges, 2L, "d", 0L, 0);
        // in edges (d.2.0 0,c.2.1 0)
        assertEquals(2, inEdges.size());
        testEdge(inEdges, 2L, "d", 0L, 0);
        testEdge(inEdges, 2L, "c", 1L, 0);
        // graphs (1 1)
        assertEquals(1, graphs.size());
        assertTrue(graphs.contains(1L));
      } else {
        assertTrue(false);
      }
    }
  }

  private void testEdge(List<Edge> edges, Long expectedOtherID,
                        String expectedLabel, Long expectedIndex,
                        int expectedPropertyCount) {
    Edge tmpEdge = EdgeFactory.createDefaultEdge(expectedOtherID,
      expectedLabel, expectedIndex);
    assertTrue(edges.contains(tmpEdge));
    int edgeIndex = edges.indexOf(tmpEdge);
    testProperties(edges.get(edgeIndex), expectedPropertyCount);
  }

  private void testProperties(Attributed v, int expectedSize) {
    int count = 0;
    for (String propertyKey : v.getPropertyKeys()) {
      switch (propertyKey) {
        case KEY_1:
          assertEquals(VALUE_1, v.getProperty(KEY_1));
          break;
        case KEY_2:
          assertEquals(VALUE_2, v.getProperty(KEY_2));
          break;
        case KEY_3:
          assertEquals(VALUE_3, v.getProperty(KEY_3));
          break;
      }
      count++;
    }
    assertEquals(expectedSize, count);
  }

  protected BufferedReader createTestReader(String[] graph)
    throws IOException {
    File tmpFile = getTempFile();
    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));

    for (String line : graph) {
      bw.write(line);
      bw.newLine();
    }
    bw.flush();
    bw.close();

    return new BufferedReader(new FileReader(tmpFile));
  }
}
