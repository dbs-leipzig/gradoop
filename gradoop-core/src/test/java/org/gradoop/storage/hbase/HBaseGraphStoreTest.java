//package org.gradoop.storage.hbase;
//
//import com.google.common.collect.Lists;
//import org.gradoop.GConstants;
//import org.gradoop.GradoopClusterTest;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.GraphData;
//import org.gradoop.model.VertexData;
//import org.gradoop.model.impl.GraphDataFactory;
//import org.gradoop.model.impl.VertexDataFactory;
//import org.gradoop.storage.GraphStore;
//import org.gradoop.storage.exceptions.UnsupportedTypeException;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.Assert.*;
//
//public class HBaseGraphStoreTest extends GradoopClusterTest {
//
//  private Iterable<GraphData> createGraphs() {
//    List<GraphData> graphDatas = new ArrayList<>();
//
//    // graph 0
//    Long graphID = 0L;
//    String graphLabel = "A";
//    Map<String, Object> graphProperties = new HashMap<>();
//    graphProperties.put("k1", "v1");
//    graphProperties.put("k2", "v2");
//    List<Long> vertices = new ArrayList<>();
//    vertices.add(0L);
//    vertices.add(1L);
//
//    graphDatas.add(GraphDataFactory
//      .createDefaultGraph(graphID, graphLabel, graphProperties, vertices));
//
//    // graph 1
//    graphID = 1L;
//    graphLabel = "A";
//    graphProperties = new HashMap<>();
//    graphProperties.put("k1", "v1");
//    vertices = new ArrayList<>();
//    vertices.add(1L);
//    vertices.add(2L);
//
//    graphDatas.add(GraphDataFactory
//      .createDefaultGraph(graphID, graphLabel, graphProperties, vertices));
//
//    return graphDatas;
//  }
//
//  @Test
//  public void writeCloseOpenReadTest() throws InterruptedException, IOException,
//    ClassNotFoundException {
//    GraphStore graphStore = createEmptyGraphStore();
//
//    // storage some data
//    for (VertexData v : createExtendedGraphVertices()) {
//      graphStore.writeVertex(v);
//    }
//
//    for (GraphData g : createGraphs()) {
//      graphStore.writeGraph(g);
//    }
//
//    // re-open
//    graphStore.close();
//    graphStore = openGraphStore();
//
//    // validate
//    validateGraphs(graphStore);
//    validateSingleVertices(graphStore);
//    validateAllVertices(graphStore);
//    validateEdges(graphStore);
//    graphStore.close();
//  }
//
//  @Test
//  public void writeFlushReadEdgesTest() {
//    GraphStore graphStore = createEmptyGraphStore();
//    graphStore.setAutoFlush(false);
//    for (VertexData v : createExtendedGraphVertices()) {
//      graphStore.writeVertex(v);
//    }
//    graphStore.flush();
//
//    validateEdges(graphStore);
//    graphStore.close();
//  }
//
//  @Test
//  public void writeFlushReadTest() {
//    GraphStore graphStore = createEmptyGraphStore();
//    graphStore.setAutoFlush(false);
//
//    // store some data
//    for (VertexData v : createExtendedGraphVertices()) {
//      graphStore.writeVertex(v);
//    }
//    for (GraphData g : createGraphs()) {
//      graphStore.writeGraph(g);
//    }
//
//    // flush changes
//    graphStore.flush();
//
//    // validate
//    validateGraphs(graphStore);
//    validateSingleVertices(graphStore);
//    graphStore.close();
//  }
//
//  private void validateGraphs(GraphStore graphStore) {
//    // g0
//    GraphData g = graphStore.readGraph(0L);
//    assertNotNull(g);
//    assertEquals("A", g.getLabel());
//    List<Long> vertices = Lists.newArrayList(g.getVertices());
//    assertEquals(2, vertices.size());
//    assertTrue(vertices.contains(0L));
//    assertTrue(vertices.contains(1L));
//    List<String> propertyKeys = Lists.newArrayList(g.getPropertyKeys());
//    assertEquals(2, propertyKeys.size());
//    for (String key : propertyKeys) {
//      if (key.equals("k1")) {
//        assertEquals("v1", g.getProperty("k1"));
//      } else if (key.equals("v2")) {
//        assertEquals("v2", g.getProperty("k2"));
//      }
//    }
//
//    // g1
//    g = graphStore.readGraph(1L);
//    assertNotNull(g);
//    assertEquals("A", g.getLabel());
//    vertices = Lists.newArrayList(g.getVertices());
//    assertEquals(2, vertices.size());
//    assertTrue(vertices.contains(1L));
//    assertTrue(vertices.contains(2L));
//    propertyKeys = Lists.newArrayList(g.getPropertyKeys());
//    assertEquals(1, propertyKeys.size());
//    assertEquals("v1", g.getProperty("k1"));
//  }
//
//  private void validateSingleVertices(GraphStore graphStore) {
//    List<VertexData> vertexDataResult =
//      Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
//    for (long l = 0L; l < EXTENDED_GRAPH.length; l++) {
//      vertexDataResult.add(graphStore.readVertex(l));
//    }
//    validateExtendedGraphVertices(vertexDataResult);
//  }
//
//  private void validateAllVertices(GraphStore graphStore) throws
//    InterruptedException, IOException, ClassNotFoundException {
//    List<VertexData> vertexDataResult = Lists.newArrayList(graphStore.getVertices(
//      GConstants.DEFAULT_TABLE_VERTICES));
//    validateExtendedGraphVertices(vertexDataResult);
//  }
//
//  private void validateEdges(GraphStore graphStore) {
//    List<EdgeData> edgeDataResult = Lists.newArrayList(graphStore.getEdges());
//    validateExtendedGraphEdges(edgeDataResult);
//  }
//
//  @Test(expected = UnsupportedTypeException.class)
//  public void wrongPropertyTypeTest() {
//    GraphStore graphStore = createEmptyGraphStore();
//
//    // list is not supported
//    final List<String> value = Lists.newArrayList();
//
//    Long vertexID = 0L;
//    final String label = "A";
//    final Map<String, Object> properties = new HashMap<>();
//    properties.put(KEY_1, value);
//
//    final Iterable<EdgeData> outEdges = Lists.newArrayListWithCapacity(0);
//    final Iterable<EdgeData> inEdges = Lists.newArrayListWithCapacity(0);
//    final Iterable<Long> graphs = Lists.newArrayList();
//    VertexData v = VertexDataFactory
//      .createDefaultVertex(vertexID, label, properties, outEdges, inEdges,
//        graphs);
//    graphStore.writeVertex(v);
//  }
//
//  @Test
//  public void propertyTypeTest() {
//    GraphStore graphStore = createEmptyGraphStore();
//
//    final int propertyCount = 6;
//    final String keyBoolean = "key1";
//    final boolean valueBoolean = true;
//    final String keyInteger = "key2";
//    final int valueInteger = 23;
//    final String keyLong = "key3";
//    final long valueLong = 42L;
//    final String keyFloat = "key4";
//    final float valueFloat = 13.37f;
//    final String keyDouble = "key5";
//    final double valueDouble = 3.14d;
//    final String keyString = "key6";
//    final String valueString = "value";
//
//    final Long vertexID = 0L;
//    final String label = "A";
//
//    final Map<String, Object> properties = new HashMap<>();
//    properties.put(keyBoolean, valueBoolean);
//    properties.put(keyInteger, valueInteger);
//    properties.put(keyLong, valueLong);
//    properties.put(keyFloat, valueFloat);
//    properties.put(keyDouble, valueDouble);
//    properties.put(keyString, valueString);
//
//    final Iterable<EdgeData> outEdges = Lists.newArrayListWithCapacity(0);
//    final Iterable<EdgeData> inEdges = Lists.newArrayListWithCapacity(0);
//    final Iterable<Long> graphs = Lists.newArrayList();
//
//    VertexData v = VertexDataFactory
//      .createDefaultVertex(vertexID, label, properties, outEdges, inEdges,
//        graphs);
//    graphStore.writeVertex(v);
//
//    // reopen
//    graphStore.close();
//    graphStore = openGraphStore();
//
//    v = graphStore.readVertex(vertexID);
//
//    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
//
//    assertEquals(propertyCount, propertyKeys.size());
//
//    for (String propertyKey : propertyKeys) {
//      switch (propertyKey) {
//      case keyBoolean:
//        assertEquals(valueBoolean, v.getProperty(propertyKey));
//        break;
//      case keyInteger:
//        assertEquals(valueInteger, v.getProperty(keyInteger));
//        break;
//      case keyLong:
//        assertEquals(valueLong, v.getProperty(keyLong));
//        break;
//      case keyFloat:
//        assertEquals(valueFloat, v.getProperty(keyFloat));
//        break;
//      case keyDouble:
//        assertEquals(valueDouble, v.getProperty(keyDouble));
//        break;
//      case keyString:
//        assertEquals(valueString, v.getProperty(keyString));
//        break;
//      }
//    }
//  }
//}