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
package org.gradoop.storage.impl.hbase.io;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Identifiable;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.io.HBaseDataSink;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test class for {@link HBaseDataSource} and {@link HBaseDataSink}
 */
public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {

  /**
   * A static HBase store with social media graph stored
   */
  private static HBaseEPGMStore[] epgmStores;

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data
   *
   * @throws IOException on failure
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
   * Test the configuration of the stores.
   */
  @Test(dataProvider = "store index")
  public void testConfig(int storeIndex) {
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
   *
   * @throws IOException on failure
   */
  @Test(dataProvider = "store index")
  public void testReadFromSource(int storeIndex) throws Exception {
    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new HBaseDataSource(epgmStores[storeIndex], getConfig())
      .getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateElementCollections(getSocialVertices(), loadedVertices);
    validateGraphElementCollections(getSocialVertices(), loadedVertices);
    validateElementCollections(getSocialEdges(), loadedEdges);
    validateGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with empty predicates
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadFromSourceWithEmptyPredicates(int storeIndex) throws Exception {
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

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateElementCollections(getSocialVertices(), loadedVertices);
    validateGraphElementCollections(getSocialVertices(), loadedVertices);
    validateElementCollections(getSocialEdges(), loadedEdges);
    validateGraphElementCollections(getSocialEdges(), loadedEdges);

  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with graph head id predicates
   */
  @Test(dataProvider = "store index")
  public void testReadWithGraphIdPredicate(int storeIndex) throws Throwable {
    List<EPGMGraphHead> testGraphs = new ArrayList<>(getSocialGraphHeads())
      .subList(1, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testGraphs.stream().map(Identifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    source = source.applyGraphPredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(testGraphs, loadedGraphHeads);
    validateElementCollections(getSocialVertices(), loadedVertices);
    validateGraphElementCollections(getSocialVertices(), loadedVertices);
    validateElementCollections(getSocialEdges(), loadedEdges);
    validateGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with vertex id predicates
   */
  @Test(dataProvider = "store index")
  public void testReadWithVertexIdPredicate(int storeIndex) throws Throwable {
    List<EPGMVertex> testVertices = new ArrayList<>(getSocialVertices())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testVertices.stream().map(Identifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply EPGMVertex-Id predicate
    source = source.applyVertexPredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateElementCollections(testVertices, loadedVertices);
    validateGraphElementCollections(testVertices, loadedVertices);
    validateElementCollections(getSocialEdges(), loadedEdges);
    validateGraphElementCollections(getSocialEdges(), loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource} with edge id predicates
   */
  @Test(dataProvider = "store index")
  public void testReadWithEdgeIdPredicate(int storeIndex) throws Throwable {
    List<EPGMEdge> testEdges = new ArrayList<>(getSocialEdges())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testEdges.stream().map(Identifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply EPGMEdge-Id predicate
    source = source.applyEdgePredicate(
      Query.elements()
        .fromSets(ids)
        .noFilter());

    assertTrue(source.isFilterPushedDown());

    GraphCollection graphCollection = source.getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(getSocialGraphHeads(), loadedGraphHeads);
    validateElementCollections(getSocialVertices(), loadedVertices);
    validateGraphElementCollections(getSocialVertices(), loadedVertices);
    validateElementCollections(testEdges, loadedEdges);
    validateGraphElementCollections(testEdges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBaseLabelIn} predicate on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithLabelInPredicate(int storeIndex) throws Exception {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_HAS_MODERATOR) ||
        e.getLabel().equals(LABEL_HAS_MEMBER))
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
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

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBaseLabelReg} predicate on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithLabelRegPredicate(int storeIndex) throws Exception {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = Lists.newArrayList(getSocialGraphHeads())
      .stream()
      .filter(g -> PATTERN_GRAPH.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<EPGMEdge> edges = Lists.newArrayList(getSocialEdges())
      .stream()
      .filter(e -> PATTERN_EDGE.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<EPGMVertex> vertices = Lists.newArrayList(getSocialVertices())
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

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropEquals} predicate on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithPropEqualsPredicate(int storeIndex) throws Exception {
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

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropLargerThan} predicate on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithPropLargerThanPredicate(int storeIndex) throws Exception {
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

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with a {@link HBasePropReg} predicate on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithPropRegPredicate(int storeIndex) throws Exception {
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

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    assertEquals(loadedGraphHeads.size(), 1);
    assertEquals(loadedEdges.size(), 2);
    assertEquals(loadedVertices.size(), 2);

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   * with logical chained predicates on each graph element
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testReadWithChainedPredicates(int storeIndex) throws Exception {
    // Extract parts of social graph to filter for
    List<EPGMGraphHead> graphHeads = getSocialGraphHeads()
      .stream()
      .filter(g -> g.getLabel().equals("Community"))
      .filter(g -> !g.getPropertyValue(PROP_INTEREST).getString().equals("Hadoop") &&
        !g.getPropertyValue(PROP_INTEREST).getString().equals("Graphs"))
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

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStores[storeIndex], getConfig());

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll()
        // WHERE gh.label = "Community" AND NOT(gh.interest = "Hadoop" OR gh.interest = "Graphs")
        .where(HBaseFilters.<EPGMGraphHead>labelIn("Community")
          .and(HBaseFilters.<EPGMGraphHead>propEquals(PROP_INTEREST, "Hadoop")
            .or(HBaseFilters.propEquals(PROP_INTEREST, "Graphs")).negate())));

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll()
        // WHERE edge.label LIKE '^has.*$' OR edge.since < 2015
        .where(HBaseFilters.<EPGMEdge>labelReg(PATTERN_EDGE)
          .or(HBaseFilters.<EPGMEdge>propLargerThan(PROP_SINCE, 2015, true).negate())));

    // Apply vertex predicate
    final HBaseElementFilter<EPGMVertex> vertexFilter = HBaseFilters.<EPGMVertex>labelIn("Person")
      .and(HBaseFilters.<EPGMVertex>propEquals(PROP_NAME, vertices.get(0).getPropertyValue("name"))
        .or(HBaseFilters.<EPGMVertex>propEquals(PROP_NAME, vertices.get(1).getPropertyValue("name"))
          .or(HBaseFilters.propEquals(PROP_NAME, vertices.get(2).getPropertyValue("name")))));

    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll()
        .where(vertexFilter));

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<EPGMVertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<EPGMEdge> loadedEdges = graphCollection.getEdges().collect();

    assertEquals(loadedGraphHeads.size(), 1);
    assertEquals(loadedEdges.size(), 21);
    assertEquals(loadedVertices.size(), 3);

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test writing a graph to {@link HBaseDataSink}
   *
   * @throws Exception on failure
   */
  @Test(dataProvider = "store index")
  public void testWriteToSink(int storeIndex) throws Exception {
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
    validateElementCollections(
      loader.getGraphHeads(),
      newStore.getGraphSpace().readRemainsAndClose()
    );
    // vertices
    validateElementCollections(
      loader.getVertices(),
      newStore.getVertexSpace().readRemainsAndClose()
    );
    validateGraphElementCollections(
      loader.getVertices(),
      newStore.getVertexSpace().readRemainsAndClose()
    );
    // edges
    validateElementCollections(
      loader.getEdges(),
      newStore.getEdgeSpace().readRemainsAndClose()
    );
    validateGraphElementCollections(
      loader.getEdges(),
      newStore.getEdgeSpace().readRemainsAndClose()
    );
  }

  /**
   * Test writing a graph to {@link HBaseDataSink} with overwrite flag, that results in an exception
   *
   * @throws Exception on failure
   */
  @Test
  public void testWriteToSinkWithOverWrite() throws Exception {
    // Create an empty store
    HBaseEPGMStore store = createEmptyEPGMStore("testWriteToSinkWithOverwrite");
    // Get a test graph and write it to the store.
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    LogicalGraph testGraph = loader.getLogicalGraphByVariable("g0");
    testGraph.writeTo(new HBaseDataSink(store, getConfig()), false);
    getExecutionEnvironment().execute();
    // Now write a different graph with overwrite and validate it by reading it again.
    LogicalGraph testGraph2 = loader.getLogicalGraphByVariable("g1");
    testGraph2.writeTo(new HBaseDataSink(store, getConfig()), true);
    getExecutionEnvironment().execute();
    collectAndAssertTrue(new HBaseDataSource(store, getConfig()).getLogicalGraph()
      .equalsByElementData(testGraph2));
    store.close();
  }
}
