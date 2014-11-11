package org.biiig.epg.store.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.biiig.epg.model.Graph;
import org.biiig.epg.model.Vertex;
import org.biiig.epg.model.impl.SimpleGraph;
import org.biiig.epg.model.impl.SimpleVertex;
import org.biiig.epg.store.GraphStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class HBaseGraphStoreTest {

  private static HBaseTestingUtility utility;

  private Iterable<Vertex> createVertices() {
    List<Vertex> vertices = new ArrayList<>();
    // vertex 0
    long vertexID = 0L;
    List<String> vertexLabels = Arrays.asList("A");
    Map<String, Object> vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    vertexProperties.put("k2", "v2");
    vertexProperties.put("k3", "v3");
    // outgoing edges
    Map<String, Map<String, Object>> outEdges = new HashMap<>();
    // 0 -> 1
    String edgeID = "a.1.0";
    Map<String, Object> edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    outEdges.put(edgeID, edgeProperties);
    // incoming edges
    Map<String, Map<String, Object>> inEdges = new HashMap<>();
    // 0 <- 1
    edgeID = "b.1.0";
    edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    edgeProperties.put("k2", "v2");
    inEdges.put(edgeID, edgeProperties);
    // graphs
    List<Long> graphs = Arrays.asList(0L);

    vertices
        .add(new SimpleVertex(vertexID, vertexLabels, vertexProperties, outEdges, inEdges, graphs));

    // vertex 1
    vertexID = 1L;
    vertexLabels = Arrays.asList("A", "B");
    vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    vertexProperties.put("k2", "v2");
    // outgoing edges
    outEdges = new HashMap<>();
    // 1 -> 0
    edgeID = "b.0.0";
    edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    edgeProperties.put("k2", "v2");
    outEdges.put(edgeID, edgeProperties);
    // 1 -> 2
    edgeID = "c.2.1";
    edgeProperties = new HashMap<>();
    outEdges.put(edgeID, edgeProperties);
    // incoming edges
    inEdges = new HashMap<>();
    // 1 <- 0
    edgeID = "a.0.0";
    edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    inEdges.put(edgeID, edgeProperties);
    // graphs
    graphs = Arrays.asList(0L, 1L);

    vertices
        .add(new SimpleVertex(vertexID, vertexLabels, vertexProperties, outEdges, inEdges, graphs));

    // vertex 2
    vertexID = 2L;
    vertexLabels = Arrays.asList("A");
    vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    vertexProperties.put("k2", "v2");
    // outgoing edges
    outEdges = new HashMap<>();
    // 2 -> 2
    edgeID = "d.2.0";
    edgeProperties = new HashMap<>();
    outEdges.put(edgeID, edgeProperties);
    // incoming edges
    inEdges = new HashMap<>();
    // 2 <- 1
    edgeID = "d.2.0";
    edgeProperties = new HashMap<>();
    inEdges.put(edgeID, edgeProperties);
    // 2 <- 2
    edgeID = "c.2.1";
    edgeProperties = new HashMap<>();
    inEdges.put(edgeID, edgeProperties);
    // graphs
    graphs = Arrays.asList(1L);

    vertices
        .add(new SimpleVertex(vertexID, vertexLabels, vertexProperties, outEdges, inEdges, graphs));

    return vertices;
  }

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

    graphs.add(new SimpleGraph(graphID, graphLabels, graphProperties, vertices));

    // graph 1
    graphID = 1L;
    graphLabels = Arrays.asList("A", "B");
    graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    vertices = new ArrayList<>();
    vertices.add(1L);
    vertices.add(2L);

    graphs.add(new SimpleGraph(graphID, graphLabels, graphProperties, vertices));

    return graphs;
  }

  private GraphStore createEmptyGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new InOutEdgesGraphsVertexHandler();
    GraphHandler graphsHandler = new BasicGraphHandler();

    HBaseGraphStoreFactory.deleteGraphStore(config);
    return HBaseGraphStoreFactory.createGraphStore(config, verticesHandler, graphsHandler);
  }

  private GraphStore openBasicGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new InOutEdgesGraphsVertexHandler();
    GraphHandler graphsHandler = new BasicGraphHandler();
    return HBaseGraphStoreFactory.createGraphStore(config, verticesHandler, graphsHandler);
  }

  @BeforeClass
  public static void setup() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniCluster();
  }

  @Test
  public void simpleTest() {
    GraphStore graphStore = createEmptyGraphStore();

    // store some data
    for (Vertex v : createVertices()) {
      graphStore.writeVertex(v);
    }

    for (Graph g : createGraphs()) {
      graphStore.writeGraph(g);
    }

    // re-open
    graphStore.close();
    graphStore = openBasicGraphStore();

    // check data

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

    // v0
    Vertex v = graphStore.readVertex(0L);
    assertNotNull(v);
    labels = Lists.newArrayList(v.getLabels());
    assertEquals(1, labels.size());
    assertTrue(labels.contains("A"));
    propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(3, propertyKeys.size());

    for (String key : propertyKeys) {
      switch (key) {
      case "k1":
        assertEquals("v1", v.getProperty("k1"));
        break;
      case "k2":
        assertEquals("v2", v.getProperty("k2"));
        break;
      case "k3":
        assertEquals("v3", v.getProperty("k3"));
        break;
      }
    }

    Map<String, Map<String, Object>> outEdges = v.getOutgoingEdges();
    assertEquals(1, outEdges.size());
    assertTrue(outEdges.containsKey("a.1.0"));
    Map<String, Map<String, Object>> inEdges = v.getIncomingEdges();
    assertEquals(1, inEdges.size());
    assertTrue(inEdges.containsKey("b.1.0"));

    List<Long> graphs = Lists.newArrayList(v.getGraphs());
    assertEquals(1, graphs.size());
    assertTrue(graphs.contains(0L));

    // v1
    v = graphStore.readVertex(1L);
    assertNotNull(v);
    labels = Lists.newArrayList(v.getLabels());
    assertEquals(2, labels.size());
    assertTrue(labels.contains("A"));
    assertTrue(labels.contains("B"));
    propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(2, propertyKeys.size());

    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", v.getProperty("k1"));
      } else if (key.equals("k2")) {
        assertEquals("v2", v.getProperty("k2"));
      }
    }

    outEdges = v.getOutgoingEdges();
    assertEquals(2, outEdges.size());
    assertTrue(outEdges.containsKey("b.0.0"));
    assertTrue(outEdges.containsKey("c.2.1"));
    inEdges = v.getIncomingEdges();
    assertEquals(1, inEdges.size());
    assertTrue(inEdges.containsKey("a.0.0"));

    graphs = Lists.newArrayList(v.getGraphs());
    assertEquals(2, graphs.size());
    assertTrue(graphs.contains(0L));
    assertTrue(graphs.contains(1L));

    // v2
    v = graphStore.readVertex(2L);
    assertNotNull(v);
    labels = Lists.newArrayList(v.getLabels());
    assertEquals(1, labels.size());
    assertTrue(labels.contains("A"));
    propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(2, propertyKeys.size());

    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", v.getProperty("k1"));
      } else if (key.equals("k2")) {
        assertEquals("v2", v.getProperty("k2"));
      }
    }

    outEdges = v.getOutgoingEdges();
    assertEquals(1, outEdges.size());
    assertTrue(outEdges.containsKey("d.2.0"));
    inEdges = v.getIncomingEdges();
    assertEquals(2, inEdges.size());
    assertTrue(inEdges.containsKey("d.2.0"));
    assertTrue(inEdges.containsKey("c.2.1"));

    graphs = Lists.newArrayList(v.getGraphs());
    assertEquals(1, graphs.size());
    assertTrue(graphs.contains(1L));

    graphStore.close();
  }

  @Test
  public void propertiesTest() {
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

    Vertex v = new SimpleVertex(vertexID, labels, properties, outEdges, inEdges, graphs);

    // reopen
    graphStore.writeVertex(v);
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