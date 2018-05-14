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
package org.gradoop.flink.io.impl.hbase;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.filter.EdgeIdIn;
import org.gradoop.flink.io.filter.Expression;
import org.gradoop.flink.io.filter.VertexIdIn;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.EPGMDatabase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopHBaseTestBase.createEmptyEPGMStore;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentEdges;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentGraphHeads;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentVertices;

/**
 * Tests for HBaseDataSource class
 */
public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {

  /**
   * Test reading from HBase
   *
   * @throws Exception
   */
  @Test
  public void testRead() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    List<PersistentVertex<Edge>> vertices = Lists.newArrayList(getSocialPersistentVertices());
    List<PersistentEdge<Vertex>> edges = Lists.newArrayList(getSocialPersistentEdges());
    List<PersistentGraphHead> graphHeads = Lists.newArrayList(getSocialPersistentGraphHeads());

    // write social graph to HBase
    for (PersistentGraphHead g : graphHeads) {
      epgmStore.writeGraphHead(g);
    }
    for (PersistentVertex<Edge> v : vertices) {
      epgmStore.writeVertex(v);
    }
    for (PersistentEdge<Vertex> e : edges) {
      epgmStore.writeEdge(e);
    }

    epgmStore.flush();

    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new HBaseDataSource(epgmStore, config).getGraphCollection();

    Collection<GraphHead> loadedGraphHeads    = Lists.newArrayList();
    Collection<Vertex>    loadedVertices      = Lists.newArrayList();
    Collection<Edge>      loadedEdges         = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);

    epgmStore.close();
  }

  /**
   * Test reading from HBase with predicate pushdown
   *
   * @throws Exception
   */
  @Test
  public void testReadWithFilter() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    // create empty EPGM store
    HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore =
      createEmptyEPGMStore(getExecutionEnvironment());

    FlinkAsciiGraphLoader graphLoader = getGraphLoader(config);

    HBaseDataSink hBaseDataSink = new HBaseDataSink(epgmStore, config);
    hBaseDataSink.write(graphLoader.getGraphCollectionByVariables("g0", "g3", "tags"));

    getExecutionEnvironment().execute();

    // create the expected graph patterns (4 cases) by taking the initial one and extend it
    FlinkAsciiGraphLoader graphLoaderExpected = getGraphLoader(config);

    graphLoaderExpected.appendToDatabaseFromString("expected_1[" +
      "(dave)" +
      "]");

    graphLoaderExpected.appendToDatabaseFromString("expected_2[" +
      "(alice)" +
      "(dave)" +
      "]");

    graphLoaderExpected.appendToDatabaseFromString("expected_3[" +
      "(gps)" +
      "(dave)" +
      "(gps)-[:hasModerator {since : 2013}]->(dave)" +
      "]");

    graphLoaderExpected.appendToDatabaseFromString("expected_4[" +
      "(gps)" +
      "(dave)" +
      "(gps)-[:hasModerator {since : 2013}]->(dave)" +
      "(gps)-[:hasMember]->(dave)" +
      "]");

    GradoopId vertexIdGps = null;
    GradoopId vertexIdDave = null;
    GradoopId vertexIdAlice = null;
    GradoopId vertexIdCarol = null;
    GradoopId edgeIdHasModerator = null;
    GradoopIdSet edgeIdsHasTag = new GradoopIdSet();

    // instantiate HBase DataSource
    HBaseDataSource dataSource = new HBaseDataSource(epgmStore, config);

    // get all necessary Gradoop-Ids from the DataSource
    for (Edge e : dataSource.getLogicalGraph().getEdges().collect()) {
      if (e.getLabel().equals("hasModerator") &&
          e.hasProperty("since") &&
          e.getPropertyValue("since").getInt() == 2013) {
        edgeIdHasModerator = e.getId();
        vertexIdGps = e.getSourceId();
        vertexIdDave = e.getTargetId();
      }
      if (e.getLabel().equals("hasTag")) {
        edgeIdsHasTag.add(e.getId());
      }
    }

    for (Vertex v : dataSource.getLogicalGraph().getVerticesByLabel("Person").collect()) {
      if (v.hasProperty("name") &&
          v.getPropertyValue("name").getString().equals("Alice")) {
        vertexIdAlice = v.getId();
      }
      if (v.hasProperty("name") &&
          v.getPropertyValue("name").getString().equals("Carol")) {
        vertexIdCarol = v.getId();
      }
    }

    if (vertexIdGps == null ||
      vertexIdDave == null ||
      edgeIdHasModerator == null ||
      vertexIdAlice == null ||
      vertexIdCarol == null ||
      edgeIdsHasTag.isEmpty()) {
      throw new RuntimeException("Necessary graph elements not found.");
    }

    // instantiate the predicates to push down (4 cases)
    List<Expression> predicatesToPushDown = new ArrayList<>();

    // Case 1: Push down a single vertex id (Dave)
    VertexIdIn vertexIds = new VertexIdIn(GradoopIdSet.fromExisting(vertexIdDave));
    predicatesToPushDown.add(vertexIds);
    DataSource dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("expected_1"))
    );

    // Case 2: Push down two independent vertex ids (Dave and Carol)
    vertexIds = new VertexIdIn(GradoopIdSet.fromExisting(vertexIdDave, vertexIdAlice));
    predicatesToPushDown.clear();
    predicatesToPushDown.add(vertexIds);
    dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("expected_2"))
    );

    // Case 3: Push down one edge (Dave and Graph Processing Forum)
    EdgeIdIn edgeIds = new EdgeIdIn(GradoopIdSet.fromExisting(edgeIdHasModerator));
    predicatesToPushDown.clear();
    predicatesToPushDown.add(edgeIds);
    dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("expected_3"))
    );

    // Case 4: Push down two connected vertex ids (Dave and Graph Processing Forum)
    vertexIds = new VertexIdIn(GradoopIdSet.fromExisting(vertexIdDave, vertexIdGps));
    predicatesToPushDown.clear();
    predicatesToPushDown.add(vertexIds);
    dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("expected_4"))
    );

    // Case 5: Push down all vertex ids from logical graph g3:Forum without edges
    vertexIds = new VertexIdIn(GradoopIdSet.fromExisting(vertexIdGps, vertexIdCarol, vertexIdDave));
    predicatesToPushDown.clear();
    predicatesToPushDown.add(vertexIds);
    dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("g3"))
    );

    // Case 6: Push down all edge ids from logical graph g0 without vertices
    edgeIds = new EdgeIdIn(edgeIdsHasTag);
    predicatesToPushDown.clear();
    predicatesToPushDown.add(edgeIds);
    dataSourceWithPredicates = dataSource.applyPredicate(predicatesToPushDown);

    collectAndAssertTrue(
      dataSourceWithPredicates.getLogicalGraph()
        .equalsByElementData(graphLoaderExpected.getLogicalGraphByVariable("tags"))
    );

    epgmStore.close();
  }

  @Test
  public void testWrite() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    // create empty EPGM store
    HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    EPGMDatabase epgmDB = loader.getDatabase();

    // write social graph to HBase via EPGM database
    epgmDB.writeTo(new HBaseDataSink(epgmStore, config));

    getExecutionEnvironment().execute();

    epgmStore.flush();

    // read social network from HBase

    // graph heads
    validateEPGMElementCollections(
      loader.getGraphHeads(),
      Lists.newArrayList(epgmStore.getGraphSpace())
    );
    // vertices
    validateEPGMElementCollections(
      loader.getVertices(),
      Lists.newArrayList(epgmStore.getVertexSpace())
    );
    validateEPGMGraphElementCollections(
      loader.getVertices(),
      Lists.newArrayList(epgmStore.getVertexSpace())
    );
    // edges
    validateEPGMElementCollections(
      loader.getEdges(),
      Lists.newArrayList(epgmStore.getEdgeSpace())
    );
    validateEPGMGraphElementCollections(
      loader.getEdges(),
      Lists.newArrayList(epgmStore.getEdgeSpace())
    );

    epgmStore.close();
  }

  /**
   * Creates a FlinkAsciiGraphLoader instance for the Tests
   *
   * @param config the GradoopFlinkConfig instance
   * @return a FlinkAsciiGraphLoader instance with loaded elements from
   *         the social network example with an additional logical graph
   *         including tag relationhips
   * @throws Exception
   */
  private static FlinkAsciiGraphLoader getGraphLoader(GradoopFlinkConfig config) throws Exception {
    InputStream inputStream = GradoopFlinkTestBase.class
        .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    FlinkAsciiGraphLoader graphLoader = new FlinkAsciiGraphLoader(config);

    graphLoader.initDatabaseFromStream(inputStream);

    graphLoader.appendToDatabaseFromString("tags[" +
        "(databases)<-[ghtd:hasTag]-(gdbs)-[ghtg1:hasTag]->" +
        "(graphs)<-[ghtg2:hasTag]-(gps)-[ghth:hasTag]->(hadoop)" +
        "]");

    return graphLoader;
  }
}
