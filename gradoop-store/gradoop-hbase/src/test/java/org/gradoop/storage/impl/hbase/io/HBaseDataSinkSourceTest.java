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
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.filter.impl.HBaseLabelIn;
import org.gradoop.storage.impl.hbase.filter.impl.HBaseLabelReg;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.*;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HBaseDataSource} and {@link HBaseDataSink}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {

  /**
   * Global Flink config for sources and sinks
   */
  private final GradoopFlinkConfig config =
    GradoopFlinkConfig.createConfig(getExecutionEnvironment());

  /**
   * A store with social media data
   */
  private HBaseEPGMStore epgmStore;

  /**
   * Instantiate the EPGMStore with a prefix and persist social media data
   */
  @Before
  public void setUp() throws IOException {
    epgmStore = openEPGMStore(getExecutionEnvironment(), "HBaseDataSinkSourceTest.");
    writeSocialGraphToStore(epgmStore);
  }

  /**
   * Close the EPGMStore after each test
   */
  @After
  public void tearDown() throws IOException {
    if (epgmStore != null) {
      epgmStore.close();
    }
  }

  /**
   * Test reading a graph collection from {@link HBaseDataSource}
   */
  @Test
  public void testReadFromSource() throws Exception {
    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new HBaseDataSource(epgmStore).getGraphCollection();

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
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore);

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

    HBaseDataSource source = new HBaseDataSource(epgmStore);

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

    HBaseDataSource source = new HBaseDataSource(epgmStore);

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

    HBaseDataSource source = new HBaseDataSource(epgmStore);

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
      .filter(e -> (e.getLabel().equals(LABEL_HAS_MODERATOR) ||
        e.getLabel().equals(LABEL_HAS_MEMBER)))
      .collect(Collectors.toList());

    List<Vertex> vertices = Lists.newArrayList(getSocialVertices())
      .stream()
      .filter(e -> (e.getLabel().equals(LABEL_TAG) || e.getLabel().equals(LABEL_FORUM)))
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore);

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(new HBaseLabelIn<>(LABEL_FORUM))
    );

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(new HBaseLabelIn<>(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER))
    );

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(new HBaseLabelIn<>(LABEL_TAG, LABEL_FORUM))
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
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore);

    // Apply empty graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(PATTERN_GRAPH))
    );

    // Apply empty edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(PATTERN_EDGE))
    );

    // Apply empty vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(PATTERN_VERTEX))
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
   * Test writing a graph to {@link HBaseDataSink}
   */
  @Test
  public void testWriteToSink() throws Exception {
    // Create an empty store
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(
      getExecutionEnvironment(),
      "testWriteToSink"
    );

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    new HBaseDataSink(epgmStore).write(epgmStore
      .getConfig()
      .getGraphCollectionFactory()
      .fromCollections(
        loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges()));

    getExecutionEnvironment().execute();

    epgmStore.flush();

    // read social network from HBase

    // graph heads
    validateEPGMElementCollections(
      loader.getGraphHeads(),
      epgmStore.getGraphSpace().readRemainsAndClose()
    );
    // vertices
    validateEPGMElementCollections(
      loader.getVertices(),
      epgmStore.getVertexSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      loader.getVertices(),
      epgmStore.getVertexSpace().readRemainsAndClose()
    );
    // edges
    validateEPGMElementCollections(
      loader.getEdges(),
      epgmStore.getEdgeSpace().readRemainsAndClose()
    );
    validateEPGMGraphElementCollections(
      loader.getEdges(),
      epgmStore.getEdgeSpace().readRemainsAndClose()
    );
  }

  /**
   * Test writing a graph to {@link HBaseDataSink} with overwrite flag, that results in an exception
   */
  @Test(expected = NotImplementedException.class)
  public void testWriteToSinkWithOverWrite() throws Exception {
    // Create an empty store
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment(),
      "testWriteToSink");

    GraphCollection graphCollection = epgmStore.getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    (new HBaseDataSink(epgmStore)).write(graphCollection, true);

    getExecutionEnvironment().execute();
  }
}
