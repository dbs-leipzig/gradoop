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
package org.gradoop.common.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.GradoopHBaseTestBase;
import org.gradoop.common.config.GradoopConfig;
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
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.gradoop.common.util.AsciiGraphLoader;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseGraphStoreTest extends GradoopHBaseTestBase {

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTest() {
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    writeGraphHead(graphStore, graphHead, vertex, edge);
    writeVertex(graphStore, vertex, edge);
    writeEdge(graphStore, vertex, edge);

    // re-open
    graphStore.close();
    graphStore = openEPGMStore(getExecutionEnvironment());

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
  public void writeFlushReadTest() {
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore = createEmptyEPGMStore(getExecutionEnvironment());
    graphStore.setAutoFlush(false);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader =
      getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    writeGraphHead(graphStore, graphHead, vertex, edge);
    writeVertex(graphStore, vertex, edge);
    writeEdge(graphStore, vertex, edge);

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
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void iteratorTest() throws InterruptedException, IOException,
    ClassNotFoundException {
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    List<PersistentVertex<Edge>> vertices =
      Lists.newArrayList(GradoopHBaseTestUtils.getSocialPersistentVertices());
    List<PersistentEdge<Vertex>> edges =
      Lists.newArrayList(GradoopHBaseTestUtils.getSocialPersistentEdges());
    List<PersistentGraphHead> graphHeads =
      Lists.newArrayList(GradoopHBaseTestUtils.getSocialPersistentGraphHeads());

    // store some data
    for (PersistentGraphHead g : graphHeads) {
      graphStore.writeGraphHead(g);
    }
    for (PersistentVertex<Edge> v : vertices) {
      graphStore.writeVertex(v);
    }
    for (PersistentEdge<Vertex> e : edges) {	
      graphStore.writeEdge(e);
    }

    graphStore.flush();

    // graph heads
    validateEPGMElementCollections(
      graphHeads,
      Lists.newArrayList(graphStore.getGraphSpace())
    );
    // vertices
    validateEPGMElementCollections(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    validateEPGMGraphElementCollections(
      vertices,
      Lists.newArrayList(graphStore.getVertexSpace())
    );
    // edges
    validateEPGMElementCollections(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );
    validateEPGMGraphElementCollections(
      edges,
      Lists.newArrayList(graphStore.getEdgeSpace())
    );

    graphStore.close();
  }

  /**
   * Tries to add an unsupported property type {@link List} as property value.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() {
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    PersistentVertexFactory<Vertex, Edge> persistentVertexFactory =
      new HBaseVertexFactory<>();
    EPGMVertexFactory<Vertex> vertexFactory = new VertexFactory();

    // Set is not supported by
    final Set<String> value = Sets.newHashSet();

    GradoopId vertexID = GradoopId.get();
    final String label = "A";
    Properties props = Properties.create();
    props.set("k1", value);

    final Set<Edge> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<Edge> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIdSet graphs = new GradoopIdSet();
    PersistentVertex<Edge> v = persistentVertexFactory.createVertex(
        vertexFactory.initVertex(vertexID, label, props, graphs),
        outEdges, inEdges);

    graphStore.writeVertex(v);
  }

  /**
   * Checks if property values are read correctly.
   */
  @Test
  public void propertyTypeTest() {
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    PersistentVertexFactory<Vertex, Edge> persistentVertexFactory =
      new HBaseVertexFactory<>();
    EPGMVertexFactory<Vertex> vertexFactory = new VertexFactory();

    final GradoopId vertexID = GradoopId.get();
    final String label = "A";

    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    final Set<Edge> outEdges = Sets.newHashSetWithExpectedSize(0);
    final Set<Edge> inEdges = Sets.newHashSetWithExpectedSize(0);
    final GradoopIdSet graphs = new GradoopIdSet();

    // write to store
    graphStore.writeVertex(persistentVertexFactory.createVertex(
      vertexFactory.initVertex(vertexID, label, properties, graphs), outEdges,
      inEdges));

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

  private AsciiGraphLoader<GraphHead, Vertex, Edge>
  getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";

    return AsciiGraphLoader.fromString(
      asciiGraph, GradoopConfig.getDefaultConfig());
  }

  private void writeGraphHead(
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore, GraphHead graphHead,
    Vertex vertex, Edge edge) {
    graphStore.writeGraphHead(new HBaseGraphHeadFactory<>().createGraphHead(
      graphHead, GradoopIdSet.fromExisting(vertex.getId()),
      GradoopIdSet.fromExisting(edge.getId())
      )
    );
  }

  private void writeVertex(HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore,
    Vertex vertex, Edge edge) {
    graphStore.writeVertex(new HBaseVertexFactory<Vertex, Edge>().createVertex(
      vertex, Sets.newHashSet(edge), Sets.newHashSet(edge)));
  }

  private void writeEdge(HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore,
    Vertex vertex, Edge edge) {
    graphStore.writeEdge(new HBaseEdgeFactory<Edge, Vertex>().createEdge(
      edge, vertex, vertex));
  }

  private void validateGraphHead(
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore,
    GraphHead originalGraphHead) {

    EPGMGraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(
    HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore,
    Vertex originalVertex) {

    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  private void validateEdge(HBaseEPGMStore<GraphHead, Vertex, Edge> graphStore,
    Edge originalEdge) {

    EPGMEdge loadedEdge = graphStore.readEdge(originalEdge.getId());

    validateEPGMElements(originalEdge, loadedEdge);
    validateEPGMGraphElements(originalEdge, loadedEdge);

    assertTrue(
      "source vertex mismatch",
      originalEdge.getSourceId().equals(
        loadedEdge.getSourceId())
    );

    assertTrue(
      "target vertex mismatch",
      originalEdge.getTargetId().equals(
        loadedEdge.getTargetId()
      )
    );
  }

}