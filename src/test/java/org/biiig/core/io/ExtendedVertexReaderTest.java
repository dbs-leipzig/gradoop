package org.biiig.core.io;

import com.google.common.collect.Lists;
import org.biiig.core.model.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExtendedVertexReaderTest {

  private static final String KEY_1 = "k1";
  private static final String KEY_2 = "k2";
  private static final String KEY_3 = "k3";
  private static final String VALUE_1 = "v1";
  private static final String VALUE_2 = "v2";
  private static final String VALUE_3 = "v3";

  @Test
  public void readExtendedGraphTest() {
    String[] graph = new String[] {
        "0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 1 k1 5 v1|1 0",
        "1|A B|2 k1 5 v1 k2 5 v2|b.0.0 2 k1 5 v1 k2 5 v2,c.2.1 0|a.0.0 1 k1 5 v1|2 0 1",
        "2|C|2 k1 5 v1 k2 5 v2|d.2.0 0|d.2.0 0,c.2.1 0|1 1"
    };

    VertexLineReader vertexLineReader = new ExtendedVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : graph) {
      vertices.add(vertexLineReader.readLine(line));
    }

    assertEquals(3, vertices.size());
    for (Vertex v : vertices) {
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