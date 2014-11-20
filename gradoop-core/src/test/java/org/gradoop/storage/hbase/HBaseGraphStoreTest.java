package org.gradoop.storage.hbase;

import com.google.common.collect.Lists;
import org.gradoop.core.ClusterBasedTest;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryGraph;
import org.gradoop.model.inmemory.MemoryVertex;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class HBaseGraphStoreTest extends ClusterBasedTest {

  private Iterable<Graph> createGraphs() {
    List<Graph> graphs = new ArrayList<>();

    // graph 0
    Long graphID = 0L;
    List<String> graphLabels = Arrays.asList("A");
    Map<String, Object> graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    graphProperties.put("k2", "v2");
    List<Long> vertices = new ArrayList<>();
    vertices.add(0L);
    vertices.add(1L);

    graphs
      .add(new MemoryGraph(graphID, graphLabels, graphProperties, vertices));

    // graph 1
    graphID = 1L;
    graphLabels = Arrays.asList("A", "B");
    graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    vertices = new ArrayList<>();
    vertices.add(1L);
    vertices.add(2L);

    graphs
      .add(new MemoryGraph(graphID, graphLabels, graphProperties, vertices));

    return graphs;
  }

  @Test
  public void simpleTest() {
    GraphStore graphStore = createEmptyGraphStore();

    // storage some data
    for (Vertex v : createExtendedGraphVertices()) {
      graphStore.writeVertex(v);
    }

    for (Graph g : createGraphs()) {
      graphStore.writeGraph(g);
    }

    // re-open
    graphStore.close();
    graphStore = openBasicGraphStore();

    // validate data
    // GRAPHS

    // g0
    Graph g = graphStore.readGraph(0L);
    assertNotNull(g);
    List<String> labels = Lists.newArrayList(g.getLabels());
    assertEquals(1, labels.size());
    assertTrue(labels.contains("A"));
    List<Long> vertices = Lists.newArrayList(g.getVertices());
    assertEquals(2, vertices.size());
    assertTrue(vertices.contains(0L));
    assertTrue(vertices.contains(1L));
    List<String> propertyKeys = Lists.newArrayList(g.getPropertyKeys());
    assertEquals(2, propertyKeys.size());
    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", g.getProperty("k1"));
      } else if (key.equals("v2")) {
        assertEquals("v2", g.getProperty("k2"));
      }
    }

    // g1
    g = graphStore.readGraph(1L);
    assertNotNull(g);
    labels = Lists.newArrayList(g.getLabels());
    assertEquals(2, labels.size());
    assertTrue(labels.contains("A"));
    assertTrue(labels.contains("B"));
    vertices = Lists.newArrayList(g.getVertices());
    assertEquals(2, vertices.size());
    assertTrue(vertices.contains(1L));
    assertTrue(vertices.contains(2L));
    propertyKeys = Lists.newArrayList(g.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", g.getProperty("k1"));

    // VERTICES
    List<Vertex> vertexResult =
      Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
    for (long l = 0L; l < EXTENDED_GRAPH.length; l++) {
      vertexResult.add(graphStore.readVertex(l));
    }
    validateExtendedGraphVertices(vertexResult);

    graphStore.close();
  }

  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() {
    GraphStore graphStore = createEmptyGraphStore();

    // list is not supported
    final List<String> value = Lists.newArrayList();

    Long vertexID = 0L;
    final Iterable<String> labels = Lists.newArrayList("A");
    final Map<String, Object> properties = new HashMap<>();
    properties.put(KEY_1, value);

    final Map<String, Map<String, Object>> outEdges = new HashMap<>();
    final Map<String, Map<String, Object>> inEdges = new HashMap<>();
    final Iterable<Long> graphs = Lists.newArrayList();
    Vertex v =
      new MemoryVertex(vertexID, labels, properties, outEdges, inEdges, graphs);
    graphStore.writeVertex(v);
  }

  @Test
  public void propertyTypeTest() {
    GraphStore graphStore = createEmptyGraphStore();

    final int propertyCount = 6;
    final String keyBoolean = "key1";
    final boolean valueBoolean = true;
    final String keyInteger = "key2";
    final int valueInteger = 23;
    final String keyLong = "key3";
    final long valueLong = 42L;
    final String keyFloat = "key4";
    final float valueFloat = 13.37f;
    final String keyDouble = "key5";
    final double valueDouble = 3.14d;
    final String keyString = "key6";
    final String valueString = "value";

    final Long vertexID = 0L;
    final Iterable<String> labels = Lists.newArrayList("A");

    final Map<String, Object> properties = new HashMap<>();
    properties.put(keyBoolean, valueBoolean);
    properties.put(keyInteger, valueInteger);
    properties.put(keyLong, valueLong);
    properties.put(keyFloat, valueFloat);
    properties.put(keyDouble, valueDouble);
    properties.put(keyString, valueString);

    final Map<String, Map<String, Object>> outEdges = new HashMap<>();
    final Map<String, Map<String, Object>> inEdges = new HashMap<>();
    final Iterable<Long> graphs = Lists.newArrayList();

    Vertex v =
      new MemoryVertex(vertexID, labels, properties, outEdges, inEdges, graphs);
    graphStore.writeVertex(v);

    // reopen
    graphStore.close();
    graphStore = openBasicGraphStore();

    v = graphStore.readVertex(vertexID);

    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());

    assertEquals(propertyCount, propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      switch (propertyKey) {
        case keyBoolean:
          assertEquals(valueBoolean, v.getProperty(propertyKey));
          break;
        case keyInteger:
          assertEquals(valueInteger, v.getProperty(keyInteger));
          break;
        case keyLong:
          assertEquals(valueLong, v.getProperty(keyLong));
          break;
        case keyFloat:
          assertEquals(valueFloat, v.getProperty(keyFloat));
          break;
        case keyDouble:
          assertEquals(valueDouble, v.getProperty(keyDouble));
          break;
        case keyString:
          assertEquals(valueString, v.getProperty(keyString));
          break;
      }
    }
  }
}