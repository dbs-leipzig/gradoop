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
package org.gradoop.common.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.GradoopHBaseTestBase;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.api.PersistentEdge;
import org.gradoop.storage.impl.hbase.api.PersistentGraphHead;
import org.gradoop.storage.impl.hbase.api.PersistentVertex;
import org.gradoop.storage.impl.hbase.api.PersistentVertexFactory;
import org.gradoop.storage.impl.hbase.factory.HBaseEdgeFactory;
import org.gradoop.storage.impl.hbase.factory.HBaseGraphHeadFactory;
import org.gradoop.storage.impl.hbase.factory.HBaseVertexFactory;
import org.gradoop.storage.impl.hbase.filter.impl.HBaseLabelIn;
import org.gradoop.storage.impl.hbase.filter.impl.HBaseLabelReg;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.common.util.AsciiGraphLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HBaseEPGMStore}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseGraphStoreTest extends GradoopHBaseTestBase {

  /**
   * A static HBase store with social media graph stored
   */
  private static HBaseEPGMStore socialNetworkStore;

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data
   */
  @BeforeClass
  public static void setUp() throws IOException {
    socialNetworkStore = openEPGMStore(getExecutionEnvironment(), "HBaseGraphStoreTest.");
    writeSocialGraphToStore(socialNetworkStore);
  }

  /**
   * Closes the static EPGMStore
   */
  @AfterClass
  public static void tearDown() throws IOException {
    if (socialNetworkStore != null) {
      socialNetworkStore.close();
    }
  }

  /**
   * Creates persistent graph, vertex and edge data. Writes data to HBase,
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment());

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
   * closes the store, opens it and reads/validates the data again.
   */
  @Test
  public void writeCloseOpenReadTestWithPrefix() throws IOException {
    String prefix = "test.";
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment(), prefix);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

    GraphHead graphHead = loader.getGraphHeads().iterator().next();
    Vertex vertex = loader.getVertices().iterator().next();
    Edge edge = loader.getEdges().iterator().next();

    writeGraphHead(graphStore, graphHead, vertex, edge);
    writeVertex(graphStore, vertex, edge);
    writeEdge(graphStore, vertex, edge);

    // re-open
    graphStore.close();
    graphStore = openEPGMStore(getExecutionEnvironment(), prefix);

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
  public void writeFlushReadTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment());
    graphStore.setAutoFlush(false);

    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = getMinimalFullFeaturedGraphLoader();

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
   * @throws IOException if read to or write from store fails
   */
  @Test
  public void iteratorTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment());

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
   * Tries to add an unsupported property type {@link Set} as property value.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    PersistentVertexFactory<Vertex, Edge> persistentVertexFactory = new HBaseVertexFactory<>();
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
  @SuppressWarnings("Duplicates")
  @Test
  public void propertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore(getExecutionEnvironment());

    PersistentVertexFactory<Vertex, Edge> persistentVertexFactory = new HBaseVertexFactory<>();
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
      }
    }

    graphStore.close();
  }

  /**
   * Test the getGraphSpace() method with an id filter predicate
   */
  @Test
  public void testGetGraphSpaceWithIdPredicate() throws IOException {
    // Fetch all graph heads from gdl file
    List<PersistentGraphHead> graphHeads = Lists.newArrayList(getSocialPersistentGraphHeads());
    // Select only a subset
    graphHeads = graphHeads.subList(1, 2);
    // Extract the graph ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(graphHeads.stream()
      .map(EPGMIdentifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<GraphHead> queryResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, queryResult);
  }

  /**
   * Test the getGraphSpace() method without an id filter predicate
   */
  @Test
  public void testGetGraphSpaceWithoutIdPredicate() throws IOException {
    // Fetch all graph heads from gdl file
    List<PersistentGraphHead> graphHeads = Lists.newArrayList(getSocialPersistentGraphHeads());
    // Query the graph store with an empty predicate
    List<GraphHead> queryResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, queryResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelIn} predicate
   */
  @Test
  public void testGetElementSpaceWithLabelInPredicate() throws IOException {
    // Extract parts of social graph to filter for
    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .stream()
      .filter(
        e -> e.getLabel().equals(LABEL_HAS_MODERATOR) || e.getLabel().equals(LABEL_HAS_MEMBER))
      .collect(Collectors.toList());

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .stream()
      .filter(e -> (e.getLabel().equals(LABEL_TAG) || e.getLabel().equals(LABEL_FORUM)))
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelIn<>(LABEL_FORUM)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelIn<>(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelIn<>(LABEL_TAG, LABEL_FORUM)))
      .readRemainsAndClose();

    validateEPGMElementCollections(testGraphs, graphHeadResult);
    validateEPGMElementCollections(testVertices, vertexResult);
    validateEPGMElementCollections(testEdges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelReg} predicate
   */
  @Test
  public void testGetElementSpaceWithLabelRegPredicate() throws IOException {
    // Extract parts of social graph to filter for
    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .stream()
      .filter(g -> PATTERN_GRAPH.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .stream()
      .filter(e -> PATTERN_EDGE.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .stream()
      .filter(v -> PATTERN_VERTEX.matcher(v.getLabel()).matches())
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelReg<>(PATTERN_GRAPH)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelReg<>(PATTERN_EDGE)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(new HBaseLabelReg<>(PATTERN_VERTEX)))
      .readRemainsAndClose();

    validateEPGMElementCollections(testGraphs, graphHeadResult);
    validateEPGMElementCollections(testVertices, vertexResult);
    validateEPGMElementCollections(testEdges, edgeResult);
  }

  /**
   * Test the getVertexSpace() method with an id filter predicate
   */
  @Test
  public void testGetVertexSpaceWithIdPredicate() throws IOException {
    // Fetch all vertices from gdl file
    List<PersistentVertex<Edge>> vertices = Lists.newArrayList(getSocialPersistentVertices());
    // Select only a subset
    vertices = vertices.subList(1, 5);

    // Extract the vertex ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(vertices.stream()
      .map(EPGMIdentifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<Vertex> queryResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(vertices, queryResult);
  }

  /**
   * Test the getVertexSpace() method without an id filter predicate
   */
  @Test
  public void testGetVertexSpaceWithoutIdPredicate() throws IOException {
    // Fetch all vertices from gdl file
    List<PersistentVertex<Edge>> vertices = Lists.newArrayList(getSocialPersistentVertices());
    // Query the graph store with an empty predicate
    List<Vertex> queryResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(vertices, queryResult);
  }

  /**
   * Test the getEdgeSpace() method with an id filter predicate
   */
  @Test
  public void testGetEdgeSpaceWithIdPredicate() throws IOException {
    // Fetch all edges from gdl file
    List<PersistentEdge<Vertex>> edges = Lists.newArrayList(getSocialPersistentEdges());
    // Select only a subset
    edges = edges.subList(3, 8);
    // Extract the edge ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(edges.stream()
      .map(EPGMIdentifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<Edge> queryResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(edges, queryResult);
  }

  /**
   * Test the getEdgeSpace() method without an id filter predicate
   */
  @Test
  public void testGetEdgeSpaceWithoutIdPredicate() throws IOException {
    // Fetch all edges from gdl file
    List<PersistentEdge<Vertex>> edges = Lists.newArrayList(getSocialPersistentEdges());
    // Query the graph store with an empty predicate
    List<Edge> queryResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(edges, queryResult);
  }

  private AsciiGraphLoader<GraphHead, Vertex, Edge>
  getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";

    return AsciiGraphLoader.fromString(
      asciiGraph, GradoopConfig.getDefaultConfig());
  }

  private void writeGraphHead(
    HBaseEPGMStore graphStore,
    GraphHead graphHead,
    Vertex vertex,
    Edge edge
  ) throws IOException {
    graphStore.writeGraphHead(new HBaseGraphHeadFactory<>().createGraphHead(
      graphHead, GradoopIdSet.fromExisting(vertex.getId()),
      GradoopIdSet.fromExisting(edge.getId())
      )
    );
  }

  private void writeVertex(
    HBaseEPGMStore graphStore,
    Vertex vertex,
    Edge edge
  ) throws IOException {
    graphStore.writeVertex(new HBaseVertexFactory<Vertex, Edge>().createVertex(
      vertex, Sets.newHashSet(edge), Sets.newHashSet(edge)));
  }

  private void writeEdge(
    HBaseEPGMStore graphStore,
    Vertex vertex,
    Edge edge
  ) throws IOException {
    graphStore.writeEdge(new HBaseEdgeFactory<Edge, Vertex>().createEdge(
      edge, vertex, vertex));
  }

  private void validateGraphHead(
    HBaseEPGMStore graphStore,
    GraphHead originalGraphHead
  ) throws IOException {

    EPGMGraphHead loadedGraphHead = graphStore.readGraph(originalGraphHead.getId());

    validateEPGMElements(originalGraphHead, loadedGraphHead);
  }

  private void validateVertex(
    HBaseEPGMStore graphStore,
    Vertex originalVertex
  ) throws IOException {

    EPGMVertex loadedVertex = graphStore.readVertex(originalVertex.getId());

    validateEPGMElements(originalVertex, loadedVertex);
    validateEPGMGraphElements(originalVertex, loadedVertex);
  }

  @SuppressWarnings("Duplicates")
  private void validateEdge(
    HBaseEPGMStore graphStore,
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
