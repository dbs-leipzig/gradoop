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
package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.exceptions.UnsupportedTypeException;
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
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBaseLabelIn;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBaseLabelReg;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropEquals;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropLargerThan;
import org.gradoop.storage.impl.hbase.predicate.filter.impl.HBasePropReg;
import org.gradoop.storage.utils.HBaseFilters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

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
    socialNetworkStore = openEPGMStore("HBaseGraphStoreTest.");
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
   */
  @Test(expected = UnsupportedTypeException.class)
  public void wrongPropertyTypeTest() throws IOException {
    HBaseEPGMStore graphStore = createEmptyEPGMStore();

    EPGMVertexFactory<Vertex> vertexFactory = new VertexFactory();

    // Queue is not supported by
    final Queue<String> value = Queues.newPriorityQueue();

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
   */
  @SuppressWarnings("Duplicates")
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
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());
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
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());
    // Query the graph store with an empty predicate
    List<GraphHead> queryResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, queryResult);
  }

  /**
   * Test the getVertexSpace() method with an id filter predicate
   */
  @Test
  public void testGetVertexSpaceWithIdPredicate() throws IOException {
    // Fetch all vertices from gdl file
    List<Vertex> vertices = Lists.newArrayList(getSocialVertices());
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
    List<Vertex> vertices = Lists.newArrayList(getSocialVertices());
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
    List<Edge> edges = Lists.newArrayList(getSocialEdges());
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
    List<Edge> edges = Lists.newArrayList(getSocialEdges());
    // Query the graph store with an empty predicate
    List<Edge> queryResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateEPGMElementCollections(edges, queryResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelIn} predicate
   */
  @Test
  public void testGetElementSpaceWithLabelInPredicate() throws IOException {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(
        e -> e.getLabel().equals(LABEL_HAS_MODERATOR) || e.getLabel().equals(LABEL_HAS_MEMBER))
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(e -> (!e.getLabel().equals(LABEL_TAG) && !e.getLabel().equals(LABEL_FORUM)))
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelIn(LABEL_FORUM)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelIn(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<Vertex>labelIn(LABEL_TAG, LABEL_FORUM).negate()))
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelReg} predicate
   */
  @Test
  public void testGetElementSpaceWithLabelRegPredicate() throws IOException {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> PATTERN_GRAPH.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> !PATTERN_EDGE.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(v -> PATTERN_VERTEX.matcher(v.getLabel()).matches())
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelReg(PATTERN_GRAPH)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<Edge>labelReg(PATTERN_EDGE).negate()))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelReg(PATTERN_VERTEX)))
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropEquals} predicate
   */
  @Test
  public void testGetElementSpaceWithPropEqualsPredicate() throws IOException {
    // Create the expected graph elements
    PropertyValue propertyValueVertexCount = PropertyValue.create(3);
    PropertyValue propertyValueSince = PropertyValue.create(2013);
    PropertyValue propertyValueCity = PropertyValue.create("Leipzig");

    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> g.hasProperty(PROP_VERTEX_COUNT))
      .filter(g -> g.getPropertyValue(PROP_VERTEX_COUNT).equals(propertyValueVertexCount))
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> e.hasProperty(PROP_SINCE))
      .filter(e -> e.getPropertyValue(PROP_SINCE).equals(propertyValueSince))
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(v -> v.hasProperty(PROP_CITY))
      .filter(v -> v.getPropertyValue(PROP_CITY).equals(propertyValueCity))
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_VERTEX_COUNT, propertyValueVertexCount)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_SINCE, propertyValueSince)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_CITY, propertyValueCity)))
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropLargerThan} predicate
   */
  @Test
  public void testGetElementSpaceWithPropLargerThanPredicate() throws IOException {
    // Create the expected graph elements
    PropertyValue propertyValueVertexCount = PropertyValue.create(3);
    PropertyValue propertyValueSince = PropertyValue.create(2014);
    PropertyValue propertyValueAge = PropertyValue.create(30);

    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      // graph with property "vertexCount" and value >= 3
      .filter(g -> g.hasProperty(PROP_VERTEX_COUNT))
      .filter(g -> g.getPropertyValue(PROP_VERTEX_COUNT).compareTo(propertyValueVertexCount) >= 0)
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      // edge with property "since" and value > 2014
      .filter(e -> e.hasProperty(PROP_SINCE))
      .filter(e -> e.getPropertyValue(PROP_SINCE).compareTo(propertyValueSince) > 0)
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      // vertex with property "age" and value > 30
      .filter(v -> v.hasProperty(PROP_AGE))
      .filter(v -> v.getPropertyValue(PROP_AGE).compareTo(propertyValueAge) > 0)
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_VERTEX_COUNT,
          propertyValueVertexCount, true)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_SINCE, propertyValueSince, false)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_AGE, propertyValueAge, false)))
      .readRemainsAndClose();

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropReg} predicate
   */
  @Test
  public void testGetElementSpaceWithPropRegPredicate() throws IOException {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      // graph with property "name" and value matches regex ".*doop$"
      .filter(g -> g.hasProperty(PROP_INTEREST))
      .filter(g -> g.getPropertyValue(PROP_INTEREST).getString()
        .matches(PATTERN_GRAPH_PROP.pattern()))
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      // edge with property "status" and value matches regex "^start..$"
      .filter(e -> e.hasProperty(PROP_STATUS))
      .filter(e -> e.getPropertyValue(PROP_STATUS).getString().matches(PATTERN_EDGE_PROP.pattern()))
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      // vertex with property "name" and value matches regex ".*ve$"
      .filter(v -> v.hasProperty(PROP_NAME))
      .filter(v -> v.getPropertyValue(PROP_NAME).getString().matches(PATTERN_VERTEX_PROP.pattern()))
      .collect(Collectors.toList());

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_INTEREST, PATTERN_GRAPH_PROP)))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_STATUS, PATTERN_EDGE_PROP)))
      .readRemainsAndClose();

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_NAME, PATTERN_VERTEX_PROP)))
      .readRemainsAndClose();

    assertEquals(1, graphHeadResult.size());
    assertEquals(2, edgeResult.size());
    assertEquals(2, vertexResult.size());

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with complex predicates
   */
  @Test
  public void testGetElementSpaceWithChainedPredicates() throws IOException {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = getSocialGraphHeads()
      .stream()
      .filter(g -> g.getLabel().equals("Community"))
      .filter(g -> g.getPropertyValue(PROP_INTEREST).getString().equals("Hadoop") ||
          g.getPropertyValue(PROP_INTEREST).getString().equals("Graphs"))
      .collect(Collectors.toList());

    List<Edge> edges = getSocialEdges()
      .stream()
      .filter(e -> e.getLabel().matches(PATTERN_EDGE.pattern()) ||
        (e.hasProperty(PROP_SINCE) && e.getPropertyValue(PROP_SINCE).getInt() < 2015))
      .collect(Collectors.toList());

    List<Vertex> vertices = getSocialVertices()
      .stream()
      .filter(v -> v.getLabel().equals("Person"))
      .collect(Collectors.toList())
      .subList(1, 4);

    // Query the store
    List<GraphHead> graphHeadResult = socialNetworkStore.getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<GraphHead>labelIn("Community")
          .and(HBaseFilters.<GraphHead>propEquals(PROP_INTEREST, "Hadoop")
            .or(HBaseFilters.propEquals(PROP_INTEREST, "Graphs")))))
      .readRemainsAndClose();

    List<Edge> edgeResult = socialNetworkStore.getEdgeSpace(
      Query.elements()
        .fromAll()
        // WHERE edge.label LIKE '^has.*$' OR edge.since < 2015
        .where(HBaseFilters.<Edge>labelReg(PATTERN_EDGE)
          .or(HBaseFilters.<Edge>propLargerThan(PROP_SINCE, 2015, true).negate())))
      .readRemainsAndClose();

    final HBaseElementFilter<Vertex> vertexFilter = HBaseFilters.<Vertex>labelIn("Person")
      .and(HBaseFilters.<Vertex>propEquals(PROP_NAME, vertices.get(0).getPropertyValue("name"))
        .or(HBaseFilters.<Vertex>propEquals(PROP_NAME, vertices.get(1).getPropertyValue("name"))
          .or(HBaseFilters.propEquals(PROP_NAME, vertices.get(2).getPropertyValue("name")))));

    List<Vertex> vertexResult = socialNetworkStore.getVertexSpace(
      Query.elements()
        .fromAll()
        .where(vertexFilter))
      .readRemainsAndClose();

    assertEquals(2, graphHeadResult.size());
    assertEquals(21, edgeResult.size());
    assertEquals(3, vertexResult.size());

    validateEPGMElementCollections(graphHeads, graphHeadResult);
    validateEPGMElementCollections(vertices, vertexResult);
    validateEPGMElementCollections(edges, edgeResult);
  }

  private AsciiGraphLoader<GraphHead, Vertex, Edge>
  getMinimalFullFeaturedGraphLoader() {
    String asciiGraph = ":G{k:\"v\"}[(v:V{k:\"v\"}),(v)-[:e{k:\"v\"}]->(v)]";

    return AsciiGraphLoader.fromString(
      asciiGraph, GradoopConfig.getDefaultConfig());
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
