/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

    AsciiGraphLoader<EPGMGraphHead, EPGMVertex, EPGMEdge> loader = getMinimalFullFeaturedGraphLoader();

    EPGMGraphHead graphHead = loader.getGraphHeads().iterator().next();
    EPGMVertex vertex = loader.getVertices().iterator().next();
    EPGMEdge edge = loader.getEdges().iterator().next();

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

    AsciiGraphLoader<EPGMGraphHead, EPGMVertex, EPGMEdge> loader = getMinimalFullFeaturedGraphLoader();

    EPGMGraphHead graphHead = loader.getGraphHeads().iterator().next();
    EPGMVertex vertex = loader.getVertices().iterator().next();
    EPGMEdge edge = loader.getEdges().iterator().next();

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

    AsciiGraphLoader<EPGMGraphHead, EPGMVertex, EPGMEdge> loader = getMinimalFullFeaturedGraphLoader();

    EPGMGraphHead graphHead = loader.getGraphHeads().iterator().next();
    EPGMVertex vertex = loader.getVertices().iterator().next();
    EPGMEdge edge = loader.getEdges().iterator().next();

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

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices());
    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges());
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());

    // write social graph to HBase
    for (EPGMGraphHead g : graphHeads) {
      graphStore.writeGraphHead(g);
    }
    for (EPGMVertex v : vertices) {
      graphStore.writeVertex(v);
    }
    for (EPGMEdge e : edges) {
      graphStore.writeEdge(e);
    }

    graphStore.flush();

    // graph heads
    validateElementCollections(
      graphHeads,
      graphStore.getGraphSpace().readRemainsAndClose()
    );
    // vertices
    validateElementCollections(
      vertices,
      graphStore.getVertexSpace().readRemainsAndClose()
    );
    validateGraphElementCollections(
      vertices,
      graphStore.getVertexSpace().readRemainsAndClose()
    );
    // edges
    validateElementCollections(
      edges,
      graphStore.getEdgeSpace().readRemainsAndClose()
    );
    validateGraphElementCollections(
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
  @Test(expectedExceptions = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    VertexFactory<EPGMVertex> vertexFactory = new EPGMVertexFactory();

    // Queue is not supported by
    final Queue<String> value = new PriorityQueue<>();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    Properties props = Properties.create();
    props.set("k1", value);

    final GradoopIdSet graphs = new GradoopIdSet();

    EPGMVertex vertex = vertexFactory.initVertex(vertexID, label, props, graphs);

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

    VertexFactory<EPGMVertex> vertexFactory = new EPGMVertexFactory();

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    graphStore.writeVertex(vertexFactory.initVertex(vertexID, label, properties, graphs));
    graphStore.flush();

    // read from store
    EPGMVertex v = graphStore.readVertex(vertexID);
    assert v != null;
    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(propertyKeys.size(), properties.size());

    for (String propertyKey : propertyKeys) {
      switch (propertyKey) {
      case KEY_0:
        assertTrue(v.getPropertyValue(propertyKey).isNull());
        assertEquals(v.getPropertyValue(propertyKey).getObject(), NULL_VAL_0);
        break;
      case KEY_1:
        assertTrue(v.getPropertyValue(propertyKey).isBoolean());
        assertEquals(v.getPropertyValue(propertyKey).getBoolean(), BOOL_VAL_1);
        break;
      case KEY_2:
        assertTrue(v.getPropertyValue(propertyKey).isInt());
        assertEquals(v.getPropertyValue(propertyKey).getInt(), INT_VAL_2);
        break;
      case KEY_3:
        assertTrue(v.getPropertyValue(propertyKey).isLong());
        assertEquals(v.getPropertyValue(propertyKey).getLong(), LONG_VAL_3);
        break;
      case KEY_4:
        assertTrue(v.getPropertyValue(propertyKey).isFloat());
        assertEquals(v.getPropertyValue(propertyKey).getFloat(), FLOAT_VAL_4, 0);
        break;
      case KEY_5:
        assertTrue(v.getPropertyValue(propertyKey).isDouble());
        assertEquals(v.getPropertyValue(propertyKey).getDouble(), DOUBLE_VAL_5, 0);
        break;
      case KEY_6:
        assertTrue(v.getPropertyValue(propertyKey).isString());
        assertEquals(v.getPropertyValue(propertyKey).getString(), STRING_VAL_6);
        break;
      case KEY_7:
        assertTrue(v.getPropertyValue(propertyKey).isBigDecimal());
        assertEquals(v.getPropertyValue(propertyKey).getBigDecimal(), BIG_DECIMAL_VAL_7);
        break;
      case KEY_8:
        assertTrue(v.getPropertyValue(propertyKey).isGradoopId());
        assertEquals(v.getPropertyValue(propertyKey).getGradoopId(), GRADOOP_ID_VAL_8);
        break;
      case KEY_9:
        assertTrue(v.getPropertyValue(propertyKey).isMap());
        assertEquals(v.getPropertyValue(propertyKey).getMap(), MAP_VAL_9);
        break;
      case KEY_a:
        assertTrue(v.getPropertyValue(propertyKey).isList());
        assertEquals(v.getPropertyValue(propertyKey).getList(), LIST_VAL_a);
        break;
      case KEY_b:
        assertTrue(v.getPropertyValue(propertyKey).isDate());
        assertEquals(v.getPropertyValue(propertyKey).getDate(), DATE_VAL_b);
        break;
      case KEY_c:
        assertTrue(v.getPropertyValue(propertyKey).isTime());
        assertEquals(v.getPropertyValue(propertyKey).getTime(), TIME_VAL_c);
        break;
      case KEY_d:
        assertTrue(v.getPropertyValue(propertyKey).isDateTime());
        assertEquals(v.getPropertyValue(propertyKey).getDateTime(), DATETIME_VAL_d);
        break;
      case KEY_e:
        assertTrue(v.getPropertyValue(propertyKey).isShort());
        assertEquals(v.getPropertyValue(propertyKey).getShort(), SHORT_VAL_e);
        break;
      case KEY_f:
        assertTrue(v.getPropertyValue(propertyKey).isSet());
        assertEquals(v.getPropertyValue(propertyKey).getSet(), SET_VAL_f);
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
    AsciiGraphLoader<EPGMGraphHead, EPGMVertex, EPGMEdge> loader = getMinimalFullFeaturedGraphLoader();
    checkIfStoreIsEmpty("Store was not empty before writing anything.", store);
    // Now write something to the store, check if it was written (i.e. the store is not empty).
    EPGMGraphHead graphHead = loader.getGraphHeads().iterator().next();
    EPGMVertex vertex = loader.getVertices().iterator().next();
    EPGMEdge edge = loader.getEdges().iterator().next();
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
      assertFalse(hasNext, message);
    }
  }

  private AsciiGraphLoader<EPGMGraphHead, EPGMVertex, EPGMEdge> getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";
    return AsciiGraphLoader.fromString(asciiGraph, getEPGMElementFactoryProvider());
  }

  private void validateGraphHead(HBaseEPGMStore graphStore, EPGMGraphHead originalGraphHead)
    throws IOException {

    GraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(HBaseEPGMStore graphStore, EPGMVertex originalVertex) throws IOException {

    Vertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateElements(originalVertex, loadedVertex);
    validateGraphElements(originalVertex, loadedVertex);
  }

  private void validateEdge(HBaseEPGMStore graphStore, EPGMEdge originalEdge) throws IOException {

    Edge loadedEdge = graphStore.readEdge(originalEdge.getId());

    validateElements(originalEdge, loadedEdge);
    validateGraphElements(originalEdge, loadedEdge);

    assert loadedEdge != null;
    assertEquals(loadedEdge.getSourceId(), originalEdge.getSourceId(), "source vertex mismatch");
    assertEquals(loadedEdge.getTargetId(), originalEdge.getTargetId(), "target vertex mismatch");
  }

}
