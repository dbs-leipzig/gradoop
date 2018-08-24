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
package org.gradoop.storage.impl.accumulo.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.AccumuloTestSuite;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static org.gradoop.common.GradoopTestUtils.BIG_DECIMAL_VAL_7;
import static org.gradoop.common.GradoopTestUtils.BOOL_VAL_1;
import static org.gradoop.common.GradoopTestUtils.DATETIME_VAL_d;
import static org.gradoop.common.GradoopTestUtils.DATE_VAL_b;
import static org.gradoop.common.GradoopTestUtils.DOUBLE_VAL_5;
import static org.gradoop.common.GradoopTestUtils.FLOAT_VAL_4;
import static org.gradoop.common.GradoopTestUtils.GRADOOP_ID_VAL_8;
import static org.gradoop.common.GradoopTestUtils.INT_VAL_2;
import static org.gradoop.common.GradoopTestUtils.KEY_0;
import static org.gradoop.common.GradoopTestUtils.KEY_1;
import static org.gradoop.common.GradoopTestUtils.KEY_2;
import static org.gradoop.common.GradoopTestUtils.KEY_3;
import static org.gradoop.common.GradoopTestUtils.KEY_4;
import static org.gradoop.common.GradoopTestUtils.KEY_5;
import static org.gradoop.common.GradoopTestUtils.KEY_6;
import static org.gradoop.common.GradoopTestUtils.KEY_7;
import static org.gradoop.common.GradoopTestUtils.KEY_8;
import static org.gradoop.common.GradoopTestUtils.KEY_9;
import static org.gradoop.common.GradoopTestUtils.KEY_a;
import static org.gradoop.common.GradoopTestUtils.KEY_b;
import static org.gradoop.common.GradoopTestUtils.KEY_c;
import static org.gradoop.common.GradoopTestUtils.KEY_d;
import static org.gradoop.common.GradoopTestUtils.KEY_e;
import static org.gradoop.common.GradoopTestUtils.KEY_f;
import static org.gradoop.common.GradoopTestUtils.LIST_VAL_a;
import static org.gradoop.common.GradoopTestUtils.LONG_VAL_3;
import static org.gradoop.common.GradoopTestUtils.MAP_VAL_9;
import static org.gradoop.common.GradoopTestUtils.NULL_VAL_0;
import static org.gradoop.common.GradoopTestUtils.SET_VAL_f;
import static org.gradoop.common.GradoopTestUtils.SHORT_VAL_e;
import static org.gradoop.common.GradoopTestUtils.STRING_VAL_6;
import static org.gradoop.common.GradoopTestUtils.SUPPORTED_PROPERTIES;
import static org.gradoop.common.GradoopTestUtils.TIME_VAL_c;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElements;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * accumulo graph store test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "basic_01";
  private static final String TEST02 = "basic_02";
  private static final String TEST03 = "basic_03";
  private static final String TEST04 = "basic_04";
  private static final String TEST05 = "basic_05";

  /**
   * Creates persistent graph, vertex and edge data. Writes data to Accumulo,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void test01_writeCloseOpenReadTest() throws AccumuloSecurityException, AccumuloException,
    IOException {
    GradoopAccumuloConfig config = AccumuloTestSuite.getAcConfig(TEST01);

    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    graphStore.writeGraphHead(graphHead);
    graphStore.writeVertex(vertex);
    graphStore.writeEdge(edge);

    // re-open
    graphStore.close();
    graphStore = new AccumuloEPGMStore(config);

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to Accumulo,
   * flushes the tables and reads/validates the data.
   */
  @Test
  public void test02_writeFlushReadTest() throws AccumuloSecurityException, AccumuloException,
    IOException {
    GradoopAccumuloConfig config = AccumuloTestSuite.getAcConfig(TEST02);

    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);
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
   */
  @Test
  public void test03_iteratorTest() throws IOException, AccumuloSecurityException,
    AccumuloException {
    GradoopAccumuloConfig config = AccumuloTestSuite.getAcConfig(TEST03);
    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);

    Collection<GraphHead> graphHeads = GradoopTestUtils.getSocialNetworkLoader().getGraphHeads();
    Collection<Edge> edges = GradoopTestUtils.getSocialNetworkLoader().getEdges();
    Collection<Vertex> vertices = GradoopTestUtils.getSocialNetworkLoader().getVertices();

    // store some data
    for (GraphHead g : graphHeads) {
      graphStore.writeGraphHead(g);
    }
    for (Edge e : edges) {
      graphStore.writeEdge(e);
    }
    for (Vertex v : vertices) {
      graphStore.writeVertex(v);
    }

    graphStore.flush();

    // graph heads
    validateEPGMElementCollections(graphHeads,
      graphStore.getGraphSpace().readRemainsAndClose());
    // vertices
    validateEPGMElementCollections(vertices,
      graphStore.getVertexSpace().readRemainsAndClose());
    validateEPGMGraphElementCollections(vertices,
      graphStore.getVertexSpace().readRemainsAndClose());
    // edges
    validateEPGMElementCollections(edges,
      graphStore.getEdgeSpace().readRemainsAndClose());
    validateEPGMGraphElementCollections(edges,
      graphStore.getEdgeSpace().readRemainsAndClose());

    graphStore.close();
  }

  /**
   * Tries to add an unsupported property type {@link Queue} as property value.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void test04_wrongPropertyTypeTest() throws AccumuloSecurityException, AccumuloException {
    GradoopAccumuloConfig config = AccumuloTestSuite.getAcConfig(TEST04);
    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);

    // Queue is not supported by
    final Queue<String> value = Queues.newPriorityQueue();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    Properties props = Properties.create();
    props.set("k1", value);

    final GradoopIdSet graphs = new GradoopIdSet();

    GradoopFlinkConfig flinkConfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    graphStore.writeVertex(flinkConfig
      .getVertexFactory()
      .initVertex(vertexID, label, props, graphs));
  }

  /**
   * Checks if property values are read correctly.
   */
  @SuppressWarnings("Duplicates")
  @Test
  public void test05_propertyTypeTest() throws AccumuloSecurityException, AccumuloException,
    IOException {
    GradoopAccumuloConfig config = AccumuloTestSuite.getAcConfig(TEST05);
    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    GradoopFlinkConfig flinkConfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    graphStore.writeVertex(flinkConfig
      .getVertexFactory()
      .initVertex(vertexID, label, properties, graphs));

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
      }
    }
  }

  private AsciiGraphLoader<GraphHead, Vertex, Edge> getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";

    return AsciiGraphLoader.fromString(asciiGraph, GradoopConfig.getDefaultConfig());
  }

  private void validateGraphHead(
    AccumuloEPGMStore graphStore,
    GraphHead originalGraphHead
  ) throws IOException {
    EPGMGraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(
    AccumuloEPGMStore graphStore,
    Vertex originalVertex
  ) throws IOException {
    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  @SuppressWarnings("Duplicates")
  private void validateEdge(
    AccumuloEPGMStore graphStore,
    Edge originalEdge
  ) throws IOException {
    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());
    validateEPGMElements(originalEdge, loadedEdge);
    validateEPGMGraphElements(originalEdge, loadedEdge);
    assert loadedEdge != null;
    assertEquals("source vertex mismatch",
      originalEdge.getSourceId(), loadedEdge.getSourceId());
    assertEquals("target vertex mismatch",
      originalEdge.getTargetId(), loadedEdge.getTargetId());
  }

}
