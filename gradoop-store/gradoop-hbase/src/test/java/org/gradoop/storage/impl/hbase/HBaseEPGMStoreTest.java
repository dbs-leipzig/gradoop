/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Lists;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class of {@link HBaseEPGMStore} with main I/O functionality.
 */
public class HBaseEPGMStoreTest extends GradoopHBaseTestBase {

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   *
   * @throws IOException on failure
   */
  @Test
  public void writeCloseOpenReadTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    graphStore.writeGraphHead(graphHead);
    graphStore.writeVertex(vertex);
    graphStore.writeEdge(edge);

    // re-open
    graphStore.close();
    graphStore = openEPGMStore();

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   *
   * @throws IOException on failure
   */
  @Test
  public void writeCloseOpenReadTestWithPrefix() throws IOException {
    String prefix = "test.";
    HBaseEPGMStore graphStore = createEmptyEPGMStore(prefix);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    graphStore.writeGraphHead(graphHead);
    graphStore.writeVertex(vertex);
    graphStore.writeEdge(edge);

    // re-open
    graphStore.close();
    graphStore = openEPGMStore(prefix);

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * flushes the tables and reads/validates the data.
   *
   * @throws IOException on failure
   */
  @Test
  public void writeFlushReadTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();
    graphStore.setAutoFlush(false);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    graphStore.writeGraphHead(graphHead);
    graphStore.writeVertex(vertex);
    graphStore.writeEdge(edge);

    // flush changes
    graphStore.flush();

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);

    graphStore.close();
  }

  /**
   * Stores social network data, loads it again and checks for element data
   * equality.
   *
   * @throws IOException if read to or write from store fails
   */
  @Test
  public void iteratorTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices());
    List<Edge> edges = Lists.newArrayList(getSocialEdges());
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());

    // write social graph to HBase
    for (GraphHead g : graphHeads) {
      graphStore.writeGraphHead(g);
    }
    for (Vertex v : vertices) {
      graphStore.writeVertex(v);
    }
    for (Edge e : edges) {
      graphStore.writeEdge(e);
    }

    graphStore.flush();

    // graph heads
    validateEPGMElementCollections(
      graphHeads,
      graphStore.getGraphSpace().readRemainsAndClose()
    );
    // vertices
    validateEPGMElementCollections(
      vertices,
      graphStore.getVertexSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      vertices,
      graphStore.getVertexSpace().readRemainsAndClose()
    );
    // edges
    validateEPGMElementCollections(
      edges,
      graphStore.getEdgeSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      edges,
      graphStore.getEdgeSpace().readRemainsAndClose()
    );

    graphStore.close();
  }

  /**
   * Tries to add an unsupported property type {@link Queue} as property value.
   *
   * @throws IOException on failure
   */
  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    EPGMVertexFactory<Vertex> vertexFactory = new VertexFactory();

    // Queue is not supported by
    final Queue<String> value = new PriorityQueue<>();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    Properties props = Properties.create();
    props.set("k1", value);

    final GradoopIdSet graphs = new GradoopIdSet();

    Vertex vertex = vertexFactory.initVertex(vertexID, label, props, graphs);

    graphStore.writeVertex(vertex);
  }

  /**
   * Checks if property values are read correctly.
   *
   * @throws IOException on failure
   */
  @Test
  public void propertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    EPGMVertexFactory<Vertex> vertexFactory = new VertexFactory();

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    graphStore.writeVertex(vertexFactory.initVertex(vertexID, label, properties, graphs));
    graphStore.flush();

    // read from store
    Vertex v = graphStore.readVertex(vertexID);
    assert v != null;
    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(properties.size(), propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      switch (propertyKey) {
      case KEY_0:
        assertTrue(v.getPropertyValue(propertyKey).isNull());
        assertEquals(NULL_VAL_0, v.getPropertyValue(propertyKey).getObject());
        break;
      case KEY_1:
        assertTrue(v.getPropertyValue(propertyKey).isBoolean());
        assertEquals(BOOL_VAL_1, v.getPropertyValue(propertyKey).getBoolean());
        break;
      case KEY_2:
        assertTrue(v.getPropertyValue(propertyKey).isInt());
        assertEquals(INT_VAL_2, v.getPropertyValue(propertyKey).getInt());
        break;
      case KEY_3:
        assertTrue(v.getPropertyValue(propertyKey).isLong());
        assertEquals(LONG_VAL_3, v.getPropertyValue(propertyKey).getLong());
        break;
      case KEY_4:
        assertTrue(v.getPropertyValue(propertyKey).isFloat());
        assertEquals(FLOAT_VAL_4, v.getPropertyValue(propertyKey).getFloat(), 0);
        break;
      case KEY_5:
        assertTrue(v.getPropertyValue(propertyKey).isDouble());
        assertEquals(DOUBLE_VAL_5, v.getPropertyValue(propertyKey).getDouble(), 0);
        break;
      case KEY_6:
        assertTrue(v.getPropertyValue(propertyKey).isString());
        assertEquals(STRING_VAL_6, v.getPropertyValue(propertyKey).getString());
        break;
      case KEY_7:
        assertTrue(v.getPropertyValue(propertyKey).isBigDecimal());
        assertEquals(BIG_DECIMAL_VAL_7, v.getPropertyValue(propertyKey).getBigDecimal());
        break;
      case KEY_8:
        assertTrue(v.getPropertyValue(propertyKey).isGradoopId());
        assertEquals(GRADOOP_ID_VAL_8, v.getPropertyValue(propertyKey).getGradoopId());
        break;
      case KEY_9:
        assertTrue(v.getPropertyValue(propertyKey).isMap());
        assertEquals(MAP_VAL_9, v.getPropertyValue(propertyKey).getMap());
        break;
      case KEY_a:
        assertTrue(v.getPropertyValue(propertyKey).isList());
        assertEquals(LIST_VAL_a, v.getPropertyValue(propertyKey).getList());
        break;
      case KEY_b:
        assertTrue(v.getPropertyValue(propertyKey).isDate());
        assertEquals(DATE_VAL_b, v.getPropertyValue(propertyKey).getDate());
        break;
      case KEY_c:
        assertTrue(v.getPropertyValue(propertyKey).isTime());
        assertEquals(TIME_VAL_c, v.getPropertyValue(propertyKey).getTime());
        break;
      case KEY_d:
        assertTrue(v.getPropertyValue(propertyKey).isDateTime());
        assertEquals(DATETIME_VAL_d, v.getPropertyValue(propertyKey).getDateTime());
        break;
      case KEY_e:
        assertTrue(v.getPropertyValue(propertyKey).isShort());
        assertEquals(SHORT_VAL_e, v.getPropertyValue(propertyKey).getShort());
        break;
      case KEY_f:
        assertTrue(v.getPropertyValue(propertyKey).isSet());
        assertEquals(SET_VAL_f, v.getPropertyValue(propertyKey).getSet());
        break;
      default: break;
      }
    }

    graphStore.close();
  }

  /**
   * Test the truncate tables functionality.
   */
  @Test
  public void truncateTablesTest() throws IOException {
    HBaseEPGMStore store = createEmptyEPGMStore("truncateTest");
    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();
    checkIfStoreIsEmpty("Store was not empty before writing anything.", store);
    // Now write something to the store, check if it was written (i.e. the store is not empty).
    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();
    store.writeGraphHead(graphHead);
    store.writeVertex(vertex);
    store.writeEdge(edge);
    store.flush();
    validateGraphHead(store, graphHead);
    validateVertex(store, vertex);
    validateEdge(store, edge);
    // Now truncate and check if the store if empty afterwards.
    store.truncateTables();
    checkIfStoreIsEmpty("Store was not empty after truncating.", store);
    // Finally check if we can write elements.
    store.writeGraphHead(graphHead);
    store.writeVertex(vertex);
    store.writeEdge(edge);
    store.close();
  }

  /**
   * Check if all tables of are store are empty.
   *
   * @param message The messenge used in the assertion.
   * @param store The HBase store to check.
   * @throws IOException when accessing the store fails.
   */
  private void checkIfStoreIsEmpty(String message, HBaseEPGMStore store) throws IOException {
    for (ClosableIterator<?> space : Arrays.asList(store.getGraphSpace(),
      store.getVertexSpace(), store.getEdgeSpace())) {
      boolean hasNext = space.hasNext();
      // Make sure to close the iterator before the assertion.
      space.close();
      assertFalse(message, hasNext);
    }
  }

  private AsciiGraphLoader<GraphHead, Vertex, Edge> getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";
    return AsciiGraphLoader.fromString(asciiGraph, GradoopConfig.getDefaultConfig());
  }

  private void validateGraphHead(HBaseEPGMStore graphStore, GraphHead originalGraphHead)
    throws IOException {

    EPGMGraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(HBaseEPGMStore graphStore, Vertex originalVertex) throws IOException {

    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  private void validateEdge(HBaseEPGMStore graphStore, Edge originalEdge) throws IOException {

    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());

    validateEPGMElements(originalEdge, loadedEdge);
    validateEPGMGraphElements(originalEdge, loadedEdge);

    assert loadedEdge != null;
    assertEquals("source vertex mismatch", originalEdge.getSourceId(), loadedEdge.getSourceId());
    assertEquals("target vertex mismatch", originalEdge.getTargetId(), loadedEdge.getTargetId());
  }

}
