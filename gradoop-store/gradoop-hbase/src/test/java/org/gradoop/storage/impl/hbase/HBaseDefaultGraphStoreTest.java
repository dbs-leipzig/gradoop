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
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
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
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HBaseEPGMStore}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseDefaultGraphStoreTest extends GradoopHBaseTestBase {

  /**
   * A static HBase store with social media graph stored
   */
  static HBaseEPGMStore socialNetworkStore;

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
      socialNetworkStore.dropTables();
      socialNetworkStore.close();
    }
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
      .filter(e -> !e.getLabel().equals(LABEL_TAG) && !e.getLabel().equals(LABEL_FORUM))
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
}
