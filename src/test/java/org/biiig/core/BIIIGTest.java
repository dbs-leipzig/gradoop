package org.biiig.core;

import com.google.common.collect.Lists;
import org.biiig.core.io.BasicVertexReader;
import org.biiig.core.io.ExtendedVertexReader;
import org.biiig.core.io.VertexLineReader;
import org.biiig.core.model.Vertex;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by martin on 12.11.14.
 */
public class BIIIGTest {

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

  protected File getTempFile() throws IOException {
    return getTempFile(null);
  }

  protected File getTempFile(String fileName) throws IOException {
    fileName = (fileName != null) ? fileName : getCallingMethod() + "_" + new Random().nextLong();
    return temporaryFolder.newFile(fileName);
  }

  protected static final String[] BASIC_GRAPH = new String[] {
      "0 1 2",
      "1 0 2",
      "2 1"
  };

  protected static final String[] EXTENDED_GRAPH = new String[] {
      "0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 1 k1 5 v1|1 0",
      "1|A B|2 k1 5 v1 k2 5 v2|b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0|a.0.0 1 k1 5 v1|2 0 1",
      "2|C|2 k1 5 v1 k2 5 v2|d.2.0 0|d.2.0 0,c.2.1 0|1 1"
  };

  protected List<Vertex> createBasicGraphVertices() {
    return createVertices(BASIC_GRAPH, new BasicVertexReader());
  }

  protected List<Vertex> createExtendedGraphVertices() {
    return createVertices(EXTENDED_GRAPH, new ExtendedVertexReader());
  }

  private List<Vertex> createVertices(String[] graph, VertexLineReader vertexLineReader) {
    List<Vertex> vertices = Lists.newArrayListWithCapacity(graph.length);
    for (String line : graph) {
      vertices.add(vertexLineReader.readLine(line));
    }
    return vertices;
  }

  protected void validateBasicGraphVertices(List<Vertex> vertices) {
    assertEquals(3, vertices.size());
    for (Vertex v : vertices) {
      Long i = v.getID();
      if (i.equals(0L)) {
        assertEquals(2, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("1"));
        assertTrue(v.getOutgoingEdges().containsKey("2"));
      } else if (i.equals(1L)) {
        assertEquals(2, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("0"));
        assertTrue(v.getOutgoingEdges().containsKey("2"));
      } else if (i.equals(2L)) {
        assertEquals(1, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("1"));
      }
    }
  }

  protected void validateExtendedGraphVertices(List<Vertex> result) {
    assertEquals(EXTENDED_GRAPH.length, result.size());
    for (Vertex v : result) {
      List<String> labels = Lists.newArrayList(v.getLabels());
      List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
      List<Long> graphs = Lists.newArrayList(v.getGraphs());
      Map<String, Map<String, Object>> outEdges = v.getOutgoingEdges();
      Map<String, Map<String, Object>> inEdges = v.getIncomingEdges();

      Long i = v.getID();
      if (i.equals(0L)) {
        // labels (A)
        assertEquals(1, labels.size());
        assertTrue(labels.contains("A"));
        // properties (3 k1 5 v1 k2 5 v2 k3 5 v3)
        assertEquals(3, propertyKeys.size());
        testProperties(v);
        // out edges (a.1.0 1 k1 5 v1)
        assertEquals(1, outEdges.size());
        String edgeKey = "a.1.0";
        assertTrue(outEdges.containsKey(edgeKey));
        assertEquals(1, outEdges.get(edgeKey).size());
        assertTrue(outEdges.get(edgeKey).containsKey(KEY_1));
        assertEquals(VALUE_1, outEdges.get(edgeKey).get(KEY_1));
        // in edges (b.1.0 1 k1 5 v1)
        assertEquals(1, inEdges.size());
        edgeKey = "b.1.0";
        assertTrue(inEdges.containsKey(edgeKey));
        assertEquals(1, inEdges.get(edgeKey).size());
        assertTrue(inEdges.get(edgeKey).containsKey(KEY_1));
        assertEquals(VALUE_1, inEdges.get(edgeKey).get(KEY_1));
        // graphs (1 0)
        assertEquals(1, graphs.size());
        assertTrue(graphs.contains(0L));
      } else if (i.equals(1L)) {
        // labels (A,B)
        assertEquals(2, labels.size());
        assertTrue(labels.contains("A"));
        assertTrue(labels.contains("B"));
        // properties (2 k1 5 v1 k2 5 v2)
        assertEquals(2, propertyKeys.size());
        testProperties(v);
        // out edges (b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0)
        assertEquals(2, outEdges.size());
        String edgeKey = "b.0.0";
        assertTrue(outEdges.containsKey(edgeKey));
        assertEquals(2, outEdges.get(edgeKey).size());
        assertTrue(outEdges.get(edgeKey).containsKey(KEY_1));
        assertEquals(VALUE_1, outEdges.get(edgeKey).get(KEY_1));
        assertTrue(outEdges.get(edgeKey).containsKey(KEY_2));
        assertEquals(VALUE_2, outEdges.get(edgeKey).get(KEY_2));
        edgeKey = "c.2.1";
        assertEquals(0, outEdges.get(edgeKey).size());
        // in edges (a.0.0 1 k1 5 v1)
        assertEquals(1, inEdges.size());
        edgeKey = "a.0.0";
        assertTrue(inEdges.containsKey(edgeKey));
        assertEquals(1, inEdges.get(edgeKey).size());
        assertTrue(inEdges.get(edgeKey).containsKey(KEY_1));
        assertEquals(VALUE_1, inEdges.get(edgeKey).get(KEY_1));
        // graphs (2 0 1)
        assertEquals(2, graphs.size());
        assertTrue(graphs.contains(0L));
        assertTrue(graphs.contains(1L));
      } else if (i.equals(2L)) {
        // labels (C)
        assertEquals(1, labels.size());
        assertTrue(labels.contains("C"));
        // properties (2 k1 5 v1 k2 5 v2)
        assertEquals(2, propertyKeys.size());
        testProperties(v);
        // out edges (d.2.0 0)
        assertEquals(1, outEdges.size());
        String edgeKey = "d.2.0";
        assertTrue(outEdges.containsKey(edgeKey));
        assertEquals(0, outEdges.get(edgeKey).size());
        // in edges (d.2.0 0,c.2.1 0)
        assertEquals(2, inEdges.size());
        edgeKey = "d.2.0";
        assertTrue(inEdges.containsKey(edgeKey));
        assertEquals(0, inEdges.get(edgeKey).size());
        edgeKey = "c.2.1";
        assertTrue(inEdges.containsKey(edgeKey));
        assertEquals(0, inEdges.get(edgeKey).size());
        // graphs (1 1)
        assertEquals(1, graphs.size());
        assertTrue(graphs.contains(1L));
      } else {
        assertTrue(false);
      }
    }
  }

  private void testProperties(Vertex v) {
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
    }
  }
}
