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
package org.gradoop.storage.impl.hbase.io;

import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
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
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HBaseDataSource} and {@link HBaseDataSink}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {

  /**
   * A static HBase store with social media graph stored
   */
  private static HBaseEPGMStore[] epgmStores;

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data
   */
  @BeforeClass
  public static void setUp() throws IOException {
    epgmStores = new HBaseEPGMStore[3];

    epgmStores[0] = openEPGMStore("HBaseDataSinkSourceTest.");
    writeSocialGraphToStore(epgmStores[0]);

    final GradoopHBaseConfig splitConfig = GradoopHBaseConfig.getDefaultConfig();
    splitConfig.enablePreSplitRegions(32);
    epgmStores[1] = openEPGMStore("HBaseDataSinkSourceSplitRegionTest.", splitConfig);
    writeSocialGraphToStore(epgmStores[1]);

    final GradoopHBaseConfig spreadingConfig = GradoopHBaseConfig.getDefaultConfig();
    spreadingConfig.useSpreadingByte(32);
    epgmStores[2] = openEPGMStore("HBaseDataSinkSourceSpreadingByteTest.", spreadingConfig);
    writeSocialGraphToStore(epgmStores[2]);
  }

  /**
   * Closes the static EPGMStore
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
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{0}, {1}, {2}});
  }

  /**
   * The store index to decide which store should be tested.
   */
  private int storeIndex;

  /**
   * Test class constructor creates an instance of this test
   *
   * @param storeIndex the store index to decide which store should be tested
   */
  public HBaseDataSinkSourceTest(int storeIndex) {
    this.storeIndex = storeIndex;
  }

  /**
   * Test the configuration of the stores.
   */
  @Test
  public void testConfig() {
    switch (storeIndex) {
    case 1:
      assertTrue(epgmStores[storeIndex].getConfig().getVertexHandler().isPreSplitRegions());
      assertTrue(epgmStores[storeIndex].getConfig().getEdgeHandler().isPreSplitRegions());
      assertTrue(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isPreSplitRegions());

      assertFalse(epgmStores[storeIndex].getConfig().getVertexHandler().isSpreadingByteUsed());
      assertFalse(epgmStores[storeIndex].getConfig().getEdgeHandler().isSpreadingByteUsed());
      assertFalse(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isSpreadingByteUsed());
      break;
    case 2:
      assertFalse(epgmStores[storeIndex].getConfig().getVertexHandler().isPreSplitRegions());
      assertFalse(epgmStores[storeIndex].getConfig().getEdgeHandler().isPreSplitRegions());
      assertFalse(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isPreSplitRegions());

      assertTrue(epgmStores[storeIndex].getConfig().getVertexHandler().isSpreadingByteUsed());
      assertTrue(epgmStores[storeIndex].getConfig().getEdgeHandler().isSpreadingByteUsed());
      assertTrue(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isSpreadingByteUsed());
      break;
    default:
      assertFalse(epgmStores[storeIndex].getConfig().getVertexHandler().isPreSplitRegions());
      assertFalse(epgmStores[storeIndex].getConfig().getEdgeHandler().isPreSplitRegions());
      assertFalse(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isPreSplitRegions());

      assertFalse(epgmStores[storeIndex].getConfig().getVertexHandler().isSpreadingByteUsed());
      assertFalse(epgmStores[storeIndex].getConfig().getEdgeHandler().isSpreadingByteUsed());
      assertFalse(epgmStores[storeIndex].getConfig().getGraphHeadHandler().isSpreadingByteUsed());
      break;
    }
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   */
  @Test
  public void testReadFromSource() throws Exception {
    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new HBaseDataSource(epgmStores[storeIndex], getConfig())
      .getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with empty predicates
   */
  @Test
  public void testReadFromSourceWithEmptyPredicates() throws Exception {
    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply empty graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(Query.elements().fromAll().noFilter());

    // Apply empty edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(Query.elements().fromAll().noFilter());

    // Apply empty vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(Query.elements().fromAll().noFilter());

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialEdges(), loadedEdges);

  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with graph head id predicates
   */
  @Test
  public void testReadWithGraphIdPredicate() throws Throwable {
    List<GraphHead> testGraphs = new ArrayList<>(getSocialGraphHeads())
      .subList(1, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testGraphs.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    source = source.applyGraphPredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(testGraphs, loadedGraphHeads);
    validateEPGMElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with vertex id predicates
   */
  @Test
  public void testReadWithVertexIdPredicate() throws Throwable {
    List<Vertex> testVertices = new ArrayList<>(getSocialVertices())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testVertices.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply Vertex-Id predicate
    source = source.applyVertexPredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(testVertices, loadedVertices);
    validateEPGMGraphElementCollections(testVertices, loadedVertices);
    validateEPGMElementCollections(getSocialEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with edge id predicates
   */
  @Test
  public void testReadWithEdgeIdPredicate() throws Throwable {
    List<Edge> testEdges = new ArrayList<>(getSocialEdges())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testEdges.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply Edge-Id predicate
    source = source.applyEdgePredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialVertices(), loadedVertices);
    validateEPGMElementCollections(testEdges, loadedEdges);
    validateEPGMGraphElementCollections(testEdges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBaseLabelIn} predicate on each graph element
   */
  @Test
  public void testReadWithLabelInPredicate() throws Exception {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_HAS_MODERATOR) ||
        e.getLabel().equals(LABEL_HAS_MEMBER))
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_TAG) || e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(HBaseFilters.labelIn(LABEL_FORUM))
    );

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(HBaseFilters.labelIn(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER))
    );

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(HBaseFilters.labelIn(LABEL_TAG, LABEL_FORUM))
    );

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBaseLabelReg} predicate on each graph element
   */
  @Test
  public void testReadWithLabelRegPredicate() throws Exception {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> PATTERN_GRAPH.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<Edge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> PATTERN_EDGE.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(v -> PATTERN_VERTEX.matcher(v.getLabel()).matches())
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply empty graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(HBaseFilters.labelReg(PATTERN_GRAPH)));

    // Apply empty edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(HBaseFilters.labelReg(PATTERN_EDGE)));

    // Apply empty vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(HBaseFilters.labelReg(PATTERN_VERTEX)));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropEquals} predicate on each graph element
   */
  @Test
  public void testReadWithPropEqualsPredicate() throws Exception {
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

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propEquals(PROP_VERTEX_COUNT, propertyValueVertexCount)));

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(HBaseFilters.propEquals(PROP_SINCE, propertyValueSince)));

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(HBaseFilters.propEquals(PROP_CITY, propertyValueCity)));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropLargerThan} predicate on each graph element
   */
  @Test
  public void testReadWithPropLargerThanPredicate() throws Exception {
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

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propLargerThan(PROP_VERTEX_COUNT, propertyValueVertexCount,
          true)));

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propLargerThan(PROP_SINCE, propertyValueSince, false)));

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propLargerThan(PROP_AGE, propertyValueAge, false)));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropReg} predicate on each graph element
   */
  @Test
  public void testReadWithPropRegPredicate() throws Exception {
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

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propReg(PROP_INTEREST, PATTERN_GRAPH_PROP)));

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propReg(PROP_STATUS, PATTERN_EDGE_PROP)));

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll()
        .where(HBaseFilters.propReg(PROP_NAME, PATTERN_VERTEX_PROP)));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    assertEquals(1, loadedGraphHeads.size());
    assertEquals(2, loadedEdges.size());
    assertEquals(2, loadedVertices.size());

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with logical chained predicates on each graph element
   */
  @Test
  public void testReadWithChainedPredicates() throws Exception {
    // Extract parts of social graph to filter for
    List<GraphHead> graphHeads = getSocialGraphHeads()
      .stream()
      .filter(g -> g.getLabel().equals("Community"))
      .filter(g -> !g.getPropertyValue(PROP_INTEREST).getString().equals("Hadoop") &&
        !g.getPropertyValue(PROP_INTEREST).getString().equals("Graphs"))
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

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll()
        // WHERE gh.label = "Community" AND NOT(gh.interest = "Hadoop" OR gh.interest = "Graphs")
        .where(HBaseFilters.<GraphHead>labelIn("Community")
          .and(HBaseFilters.<GraphHead>propEquals(PROP_INTEREST, "Hadoop")
            .or(HBaseFilters.propEquals(PROP_INTEREST, "Graphs")).negate())));

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll()
        // WHERE edge.label LIKE '^has.*$' OR edge.since < 2015
        .where(HBaseFilters.<Edge>labelReg(PATTERN_EDGE)
          .or(HBaseFilters.<Edge>propLargerThan(PROP_SINCE, 2015, true).negate())));

    // Apply vertex predicate
    final HBaseElementFilter<Vertex> vertexFilter = HBaseFilters.<Vertex>labelIn("Person")
      .and(HBaseFilters.<Vertex>propEquals(PROP_NAME, vertices.get(0).getPropertyValue("name"))
        .or(HBaseFilters.<Vertex>propEquals(PROP_NAME, vertices.get(1).getPropertyValue("name"))
          .or(HBaseFilters.propEquals(PROP_NAME, vertices.get(2).getPropertyValue("name")))));

    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll()
        .where(vertexFilter));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    assertEquals(1, loadedGraphHeads.size());
    assertEquals(21, loadedEdges.size());
    assertEquals(3, loadedVertices.size());

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test writing a graph to {@link HBaseDataSink}
   */
  @Test
  public void testWriteToSink() throws Exception {
    // Create an empty store
    HBaseEPGMStore newStore;

    switch (storeIndex) {
    case 1:
      final GradoopHBaseConfig splitConfig = GradoopHBaseConfig.getDefaultConfig();
      splitConfig.enablePreSplitRegions(32);
      newStore = openEPGMStore("HBaseDataSinkSplitRegionTest" + storeIndex + ".", splitConfig);
      break;
    case 2:
      final GradoopHBaseConfig spreadingConfig = GradoopHBaseConfig.getDefaultConfig();
      spreadingConfig.useSpreadingByte(32);
      newStore = openEPGMStore("HBaseDataSinkSpreadingTest" + storeIndex + ".", spreadingConfig);
      break;
    default:
      newStore = openEPGMStore("HBaseDataSinkTest" + storeIndex + ".");
      break;
    }

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    new HBaseDataSink(newStore, getConfig()).write(getConfig().getGraphCollectionFactory()
      .fromCollections(
        loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges()));

    getExecutionEnvironment().execute();

    newStore.flush();

    // read social network from HBase

    // graph heads
    validateEPGMElementCollections(
      loader.getGraphHeads(),
      newStore.getGraphSpace().readRemainsAndClose()
    );
    // vertices
    validateEPGMElementCollections(
      loader.getVertices(),
      newStore.getVertexSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      loader.getVertices(),
      newStore.getVertexSpace().readRemainsAndClose()
    );
    // edges
    validateEPGMElementCollections(
      loader.getEdges(),
      newStore.getEdgeSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      loader.getEdges(),
      newStore.getEdgeSpace().readRemainsAndClose()
    );
  }

  /**
   * Test writing a graph to {@link HBaseDataSink} with overwrite flag, that results in an exception
   */
  @Test(expected = NotImplementedException.class)
  public void testWriteToSinkWithOverWrite() throws Exception {
    // Create an empty store
    HBaseEPGMStore newStore = createEmptyEPGMStore("testWriteToSink");

    GraphCollection graphCollection = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    new HBaseDataSink(newStore, getConfig()).write(graphCollection, true);

    getExecutionEnvironment().execute();
  }
}
