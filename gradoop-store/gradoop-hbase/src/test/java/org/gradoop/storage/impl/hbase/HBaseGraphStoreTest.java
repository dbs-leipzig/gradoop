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
import org.gradoop.common.model.api.entities.Identifiable;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBaseLabelIn;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBaseLabelReg;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBasePropEquals;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBasePropLargerThan;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBasePropReg;
import org.gradoop.storage.hbase.utils.HBaseFilters;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test class for {@link HBaseEPGMStore}
 */
public class HBaseGraphStoreTest extends GradoopHBaseTestBase {

  /**
   * A static HBase store with social media graph stored
   */
  static HBaseEPGMStore[] epgmStores;

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data
   *
   * @throws IOException on failure
   */
  @BeforeClass
  public static void setUp() throws IOException {
    epgmStores = new HBaseEPGMStore[3];

    epgmStores[0] = openEPGMStore("HBaseGraphStoreTest.");
    writeSocialGraphToStore(epgmStores[0]);

    final GradoopHBaseConfig splitConfig = GradoopHBaseConfig.getDefaultConfig();
    splitConfig.enablePreSplitRegions(32);
    epgmStores[1] = openEPGMStore("HBaseGraphStoreSplitRegionTest.", splitConfig);
    writeSocialGraphToStore(epgmStores[1]);

    final GradoopHBaseConfig spreadingConfig = GradoopHBaseConfig.getDefaultConfig();
    spreadingConfig.useSpreadingByte(32);
    epgmStores[2] = openEPGMStore("HBaseGraphStoreSpreadingByteTest.", spreadingConfig);
    writeSocialGraphToStore(epgmStores[2]);
  }

  /**
   * Closes the static EPGMStore
   *
   * @throws IOException on failure
   */
  @AfterClass
  public static void tearDown() throws IOException {
    for (HBaseEPGMStore store : epgmStores) {
      if (store != null) {
        store.dropTables();
        store.close();
      }
    }
  }

  /**
   * Parameters for tests. 0 => default store config; 1 => pre-split regions; 2 => spreading byte
   *
   * @return the integer to choose the epgm store to test
   */
  @DataProvider(name = "store index")
  public static Object[][] storeIndexProvider() {
    return new Object[][] {{0}, {1}, {2}};
  }

  /**
   * Test, whether the store uses the correct region splitting.
   */
  @Test(dataProvider = "store index")
  public void testConfig(int storeIndex) {
    switch (storeIndex) {
    case 1:
      assertTrue(epgmStores[storeIndex].getConfig().getVertexHandler().isPreSplitRegions());
      assertTrue(epgmStores[storeIndex].getConfig().getEdgeHandler().isPreSplitRegions());
      assertTrue(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isPreSplitRegions());
      break;
    case 2:
      assertTrue(epgmStores[storeIndex].getConfig().getVertexHandler().isSpreadingByteUsed());
      assertTrue(epgmStores[storeIndex].getConfig().getEdgeHandler().isSpreadingByteUsed());
      assertTrue(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isSpreadingByteUsed());
      break;
    default:
    }
  }

  /**
   * Test the getGraphSpace() method with an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetGraphSpaceWithIdPredicate(int storeIndex) throws IOException {
    // Fetch all graph heads from gdl file
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());
    // Select only a subset
    graphHeads = graphHeads.subList(1, 2);
    // Extract the graph ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(graphHeads.stream()
      .map(Identifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<EPGMGraphHead> queryResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(graphHeads, queryResult);
  }

  /**
   * Test the getGraphSpace() method without an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetGraphSpaceWithoutIdPredicate(int storeIndex) throws IOException {
    // Fetch all graph heads from gdl file
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads());
    // Query the graph store with an empty predicate
    List<EPGMGraphHead> queryResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(graphHeads, queryResult);
  }

  /**
   * Test the getVertexSpace() method with an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetVertexSpaceWithIdPredicate(int storeIndex) throws IOException {
    // Fetch all vertices from gdl file
    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices());
    // Select only a subset
    vertices = vertices.subList(1, 5);

    // Extract the vertex ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(vertices.stream()
      .map(Identifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<EPGMVertex> queryResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(vertices, queryResult);
  }

  /**
   * Test the getVertexSpace() method without an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetVertexSpaceWithoutIdPredicate(int storeIndex) throws IOException {
    // Fetch all vertices from gdl file
    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices());
    // Query the graph store with an empty predicate
    List<EPGMVertex> queryResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(vertices, queryResult);
  }

  /**
   * Test the getEdgeSpace() method with an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetEdgeSpaceWithIdPredicate(int storeIndex) throws IOException {
    // Fetch all edges from gdl file
    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges());
    // Select only a subset
    edges = edges.subList(3, 8);
    // Extract the edge ids
    GradoopIdSet ids = GradoopIdSet.fromExisting(edges.stream()
      .map(Identifiable::getId)
      .collect(Collectors.toList()));
    // Query with the extracted ids
    List<EPGMEdge> queryResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromSets(ids)
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(edges, queryResult);
  }

  /**
   * Test the getEdgeSpace() method without an id filter predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetEdgeSpaceWithoutIdPredicate(int storeIndex) throws IOException {
    // Fetch all edges from gdl file
    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges());
    // Query the graph store with an empty predicate
    List<EPGMEdge> queryResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .noFilter())
      .readRemainsAndClose();

    validateElementCollections(edges, queryResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelIn} predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithLabelInPredicate(int storeIndex) throws IOException {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(
        e -> e.getLabel().equals(LABEL_HAS_MODERATOR) || e.getLabel().equals(LABEL_HAS_MEMBER))
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(e -> !e.getLabel().equals(LABEL_TAG) && !e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelIn(LABEL_FORUM)))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelIn(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER)))
      .readRemainsAndClose();

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<EPGMVertex>labelIn(LABEL_TAG, LABEL_FORUM).negate()))
      .readRemainsAndClose();

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBaseLabelReg} predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithLabelRegPredicate(int storeIndex) throws IOException {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> PATTERN_GRAPH.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> !PATTERN_EDGE.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(v -> PATTERN_VERTEX.matcher(v.getLabel()).matches())
      .collect(Collectors.toList());

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelReg(PATTERN_GRAPH)))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<EPGMEdge>labelReg(PATTERN_EDGE).negate()))
      .readRemainsAndClose();

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.labelReg(PATTERN_VERTEX)))
      .readRemainsAndClose();

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropEquals} predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithPropEqualsPredicate(int storeIndex) throws IOException {
    // Create the expected graph elements
    PropertyValue propertyValueVertexCount = PropertyValue.create(3);
    PropertyValue propertyValueSince = PropertyValue.create(2013);
    PropertyValue propertyValueCity = PropertyValue.create("Leipzig");

    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> g.hasProperty(PROP_VERTEX_COUNT))
      .filter(g -> g.getPropertyValue(PROP_VERTEX_COUNT).equals(propertyValueVertexCount))
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> e.hasProperty(PROP_SINCE))
      .filter(e -> e.getPropertyValue(PROP_SINCE).equals(propertyValueSince))
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(v -> v.hasProperty(PROP_CITY))
      .filter(v -> v.getPropertyValue(PROP_CITY).equals(propertyValueCity))
      .collect(Collectors.toList());

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_VERTEX_COUNT, propertyValueVertexCount)))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_SINCE, propertyValueSince)))
      .readRemainsAndClose();

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propEquals(PROP_CITY, propertyValueCity)))
      .readRemainsAndClose();

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropLargerThan} predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithPropLargerThanPredicate(int storeIndex) throws IOException {
    // Create the expected graph elements
    PropertyValue propertyValueVertexCount = PropertyValue.create(3);
    PropertyValue propertyValueSince = PropertyValue.create(2014);
    PropertyValue propertyValueAge = PropertyValue.create(30);

    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      // graph with property "vertexCount" and value >= 3
      .filter(g -> g.hasProperty(PROP_VERTEX_COUNT))
      .filter(g -> g.getPropertyValue(PROP_VERTEX_COUNT).compareTo(propertyValueVertexCount) >= 0)
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      // edge with property "since" and value > 2014
      .filter(e -> e.hasProperty(PROP_SINCE))
      .filter(e -> e.getPropertyValue(PROP_SINCE).compareTo(propertyValueSince) > 0)
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      // vertex with property "age" and value > 30
      .filter(v -> v.hasProperty(PROP_AGE))
      .filter(v -> v.getPropertyValue(PROP_AGE).compareTo(propertyValueAge) > 0)
      .collect(Collectors.toList());

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_VERTEX_COUNT,
          propertyValueVertexCount, true)))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_SINCE, propertyValueSince, false)))
      .readRemainsAndClose();

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propLargerThan(PROP_AGE, propertyValueAge, false)))
      .readRemainsAndClose();

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method
   * with the {@link HBasePropReg} predicate
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithPropRegPredicate(int storeIndex) throws IOException {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      // graph with property "name" and value matches regex ".*doop$"
      .filter(g -> g.hasProperty(PROP_INTEREST))
      .filter(g -> g.getPropertyValue(PROP_INTEREST).getString()
        .matches(PATTERN_GRAPH_PROP.pattern()))
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      // edge with property "status" and value matches regex "^start..$"
      .filter(e -> e.hasProperty(PROP_STATUS))
      .filter(e -> e.getPropertyValue(PROP_STATUS).getString().matches(PATTERN_EDGE_PROP.pattern()))
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      // vertex with property "name" and value matches regex ".*ve$"
      .filter(v -> v.hasProperty(PROP_NAME))
      .filter(v -> v.getPropertyValue(PROP_NAME).getString().matches(PATTERN_VERTEX_PROP.pattern()))
      .collect(Collectors.toList());

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_INTEREST, PATTERN_GRAPH_PROP)))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_STATUS, PATTERN_EDGE_PROP)))
      .readRemainsAndClose();

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.propReg(PROP_NAME, PATTERN_VERTEX_PROP)))
      .readRemainsAndClose();

    assertEquals(graphHeadResult.size(), 1);
    assertEquals(edgeResult.size(), 2);
    assertEquals(vertexResult.size(), 2);

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }

  /**
   * Test the getGraphSpace(), getVertexSpace() and getEdgeSpace() method with complex predicates
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testGetElementSpaceWithChainedPredicates(int storeIndex) throws IOException {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = getSocialGraphHeads()
      .stream()
      .filter(g -> g.getLabel().equals("Community"))
      .filter(g -> g.getPropertyValue(PROP_INTEREST).getString().equals("Hadoop") ||
          g.getPropertyValue(PROP_INTEREST).getString().equals("Graphs"))
      .collect(Collectors.toList());

    List<EPGMEdge> edges = getSocialEdges()
      .stream()
      .filter(e -> e.getLabel().matches(PATTERN_EDGE.pattern()) ||
        (e.hasProperty(PROP_SINCE) && e.getPropertyValue(PROP_SINCE).getInt() < 2015))
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = getSocialVertices()
      .stream()
      .filter(v -> v.getLabel().equals("Person"))
      .collect(Collectors.toList())
      .subList(1, 4);

    // Query the store
    List<EPGMGraphHead> graphHeadResult = epgmStores[storeIndex].getGraphSpace(
      Query.elements()
        .fromAll()
        .where(HBaseFilters.<EPGMGraphHead>labelIn("Community")
          .and(HBaseFilters.<EPGMGraphHead>propEquals(PROP_INTEREST, "Hadoop")
            .or(HBaseFilters.propEquals(PROP_INTEREST, "Graphs")))))
      .readRemainsAndClose();

    List<EPGMEdge> edgeResult = epgmStores[storeIndex].getEdgeSpace(
      Query.elements()
        .fromAll()
        // WHERE edge.label LIKE '^has.*$' OR edge.since < 2015
        .where(HBaseFilters.<EPGMEdge>labelReg(PATTERN_EDGE)
          .or(HBaseFilters.<EPGMEdge>propLargerThan(PROP_SINCE, 2015, true).negate())))
      .readRemainsAndClose();

    final HBaseElementFilter<EPGMVertex> vertexFilter = HBaseFilters.<EPGMVertex>labelIn("Person")
      .and(HBaseFilters.<EPGMVertex>propEquals(PROP_NAME, vertices.get(0).getPropertyValue("name"))
        .or(HBaseFilters.<EPGMVertex>propEquals(PROP_NAME, vertices.get(1).getPropertyValue("name"))
          .or(HBaseFilters.propEquals(PROP_NAME, vertices.get(2).getPropertyValue("name")))));

    List<EPGMVertex> vertexResult = epgmStores[storeIndex].getVertexSpace(
      Query.elements()
        .fromAll()
        .where(vertexFilter))
      .readRemainsAndClose();

    assertEquals(graphHeadResult.size(), 2);
    assertEquals(edgeResult.size(), 21);
    assertEquals(vertexResult.size(), 3);

    validateElementCollections(graphHeads, graphHeadResult);
    validateElementCollections(vertices, vertexResult);
    validateElementCollections(edges, edgeResult);
  }
}
