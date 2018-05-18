/**
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

package org.gradoop.common.storage.impl.accumulo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.gradoop.AccumuloTestSuite;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * accumulo graph store test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AccumuloGraphStoreTest extends GradoopFlinkTestBase {

  private static final String TEST01 = "store_test01.";
  private static final String TEST02 = "store_test02.";
  private static final String TEST03 = "store_test03.";
  private static final String TEST04 = "store_test04.";
  private static final String TEST05 = "store_test05.";

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void test01_writeCloseOpenReadTest() throws AccumuloSecurityException, AccumuloException {
    GradoopAccumuloConfig<GraphHead, Vertex, Edge> config = AccumuloTestSuite.getAcConfig(
      getExecutionEnvironment(), TEST01);

    AccumuloEPGMStore<GraphHead, Vertex, Edge> graphStore = new AccumuloEPGMStore<>(config);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    graphStore.writeGraphHead(graphHead);
    graphStore.writeVertex(vertex);
    graphStore.writeEdge(edge);

    // re-open
    graphStore.close();
    graphStore = new AccumuloEPGMStore<>(config);

    // validate
    validateGraphHead(graphStore, graphHead);
    validateVertex(graphStore, vertex);
    validateEdge(graphStore, edge);
    graphStore.close();
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * flushes the tables and reads/validates the data.
   */
  @Test
  public void test02_writeFlushReadTest() throws AccumuloSecurityException, AccumuloException {
    GradoopAccumuloConfig<GraphHead, Vertex, Edge> config = AccumuloTestSuite.getAcConfig(
      getExecutionEnvironment(), TEST02);

    AccumuloEPGMStore<GraphHead, Vertex, Edge> graphStore = new AccumuloEPGMStore<>(config);
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
    GradoopAccumuloConfig<GraphHead, Vertex, Edge> config = AccumuloTestSuite.getAcConfig(
      getExecutionEnvironment(), TEST03);
    AccumuloEPGMStore<GraphHead, Vertex, Edge> graphStore = new AccumuloEPGMStore<>(config);

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
    validateEPGMElementCollections(graphHeads, Lists.newArrayList(graphStore.getGraphSpace()));
    // vertices
    validateEPGMElementCollections(vertices, Lists.newArrayList(graphStore.getVertexSpace()));
    validateEPGMGraphElementCollections(vertices, Lists.newArrayList(graphStore.getVertexSpace()));
    // edges
    validateEPGMElementCollections(edges, Lists.newArrayList(graphStore.getEdgeSpace()));
    validateEPGMGraphElementCollections(edges, Lists.newArrayList(graphStore.getEdgeSpace()));

    graphStore.close();
  }

  /**
   * Tries to add an unsupported property type {@link List} as property value.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void test04_wrongPropertyTypeTest() throws AccumuloSecurityException, AccumuloException {
    GradoopAccumuloConfig<GraphHead, Vertex, Edge> config = AccumuloTestSuite.getAcConfig(
      getExecutionEnvironment(), TEST04);
    AccumuloEPGMStore<GraphHead, Vertex, Edge> graphStore = new AccumuloEPGMStore<>(config);

    // Set is not supported by
    final Set<String> value = Sets.newHashSet();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    Properties props = Properties.create();
    props.set("k1", value);

    final GradoopIdSet graphs = new GradoopIdSet();

    graphStore.writeVertex(config.getVertexFactory().initVertex(vertexID, label, props, graphs));
  }

  /**
   * Checks if property values are read correctly.
   */
  @SuppressWarnings("Duplicates")
  @Test
  public void test05_propertyTypeTest() throws AccumuloSecurityException, AccumuloException {
    GradoopAccumuloConfig<GraphHead, Vertex, Edge> config = AccumuloTestSuite.getAcConfig(getExecutionEnvironment(), TEST05);
    AccumuloEPGMStore<GraphHead, Vertex, Edge> graphStore = new AccumuloEPGMStore<>(config);

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    graphStore.writeVertex(config.getVertexFactory().initVertex(vertexID, label, properties, graphs));

    graphStore.flush();

    // read from store
    Vertex v = graphStore.readVertex(vertexID);
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
  ) {
    EPGMGraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(
    AccumuloEPGMStore graphStore,
    Vertex originalVertex
  ) {
    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  @SuppressWarnings("Duplicates")
  private void validateEdge(
    AccumuloEPGMStore graphStore,
    Edge originalEdge
  ) {
    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());
    validateEPGMElements(originalEdge, loadedEdge);
    validateEPGMGraphElements(originalEdge, loadedEdge);
    assertTrue("source vertex mismatch",
      originalEdge.getSourceId().equals(loadedEdge.getSourceId()));
    assertTrue("target vertex mismatch",
      originalEdge.getTargetId().equals(loadedEdge.getTargetId()));
  }
}
