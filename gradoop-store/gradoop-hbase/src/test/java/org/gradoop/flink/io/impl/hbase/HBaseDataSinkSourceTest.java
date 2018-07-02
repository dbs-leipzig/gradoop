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
import org.gradoop.GradoopHBaseTestBase;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.common.storage.impl.hbase.api.PersistentEdge;
import org.gradoop.common.storage.impl.hbase.api.PersistentGraphHead;
import org.gradoop.common.storage.impl.hbase.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.predicate.filter.impl.HBaseLabelIn;
import org.gradoop.common.storage.impl.hbase.predicate.filter.impl.HBaseLabelReg;
import org.gradoop.common.storage.impl.hbase.predicate.filter.impl.HBasePropEquals;
import org.gradoop.common.storage.predicate.filter.impl.LabelIn;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.gradoop.GradoopHBaseTestBase.createEmptyEPGMStore;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.*;
import static org.junit.Assert.assertTrue;

public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {

  private static final String LABEL_FORUM = "Forum";
  private static final String LABEL_TAG = "Tag";
  private static final String LABEL_HAS_MODERATOR = "hasModerator";
  private static final String LABEL_HAS_MEMBER = "hasMember";

  @Test
  public void testReadFromSource() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new HBaseDataSource(epgmStore, config).getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(getSocialPersistentGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialPersistentEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialPersistentEdges(), loadedEdges);

    epgmStore.close();
  }

  @Test
  public void testReadFromSourceWithEmptyPredicates() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore, config);

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

    validateEPGMElementCollections(getSocialPersistentGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialPersistentEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialPersistentEdges(), loadedEdges);

    epgmStore.close();
  }

  @Test
  public void testReadWithGraphIdPredicate() throws Throwable {

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = GradoopHBaseTestBase.openEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .subList(1, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testGraphs.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStore, config);

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
    validateEPGMElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMElementCollections(getSocialPersistentEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialPersistentEdges(), loadedEdges);
  }

  @Test
  public void testReadWithVertexIdPredicate() throws Throwable {

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = GradoopHBaseTestBase.createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testVertices.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStore, config);

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

    validateEPGMElementCollections(getSocialPersistentGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(testVertices, loadedVertices);
    validateEPGMGraphElementCollections(testVertices, loadedVertices);
    validateEPGMElementCollections(getSocialPersistentEdges(), loadedEdges);
    validateEPGMGraphElementCollections(getSocialPersistentEdges(), loadedEdges);
  }

  @Test
  public void testReadWithEdgeIdPredicate() throws Throwable {

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = GradoopHBaseTestBase.openEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .subList(0, 3);

    GradoopIdSet ids = GradoopIdSet.fromExisting(
      testEdges.stream().map(EPGMIdentifiable::getId).collect(Collectors.toList())
    );

    HBaseDataSource source = new HBaseDataSource(epgmStore, config);

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

    validateEPGMElementCollections(getSocialPersistentGraphHeads(), loadedGraphHeads);
    validateEPGMElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMGraphElementCollections(getSocialPersistentVertices(), loadedVertices);
    validateEPGMElementCollections(testEdges, loadedEdges);
    validateEPGMGraphElementCollections(testEdges, loadedEdges);
  }

  @Test
  public void testReadWithLabelInPredicate() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    // Extract parts of social graph to filter for
    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .stream()
      .filter(e -> e.getLabel().equals(LABEL_FORUM))
      .collect(Collectors.toList());

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .stream()
      .filter(e -> (e.getLabel().equals(LABEL_HAS_MODERATOR) ||
        e.getLabel().equals(LABEL_HAS_MEMBER)))
      .collect(Collectors.toList());

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .stream()
      .filter(e -> (e.getLabel().equals(LABEL_TAG) || e.getLabel().equals(LABEL_FORUM)))
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore, config);

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(new HBaseLabelIn<>(LABEL_FORUM))
    );

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(
        new HBaseLabelIn<>(LABEL_HAS_MODERATOR, LABEL_HAS_MEMBER)
      )
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

    validateEPGMElementCollections(testGraphs, loadedGraphHeads);
    validateEPGMElementCollections(testVertices, loadedVertices);
    validateEPGMGraphElementCollections(testVertices, loadedVertices);
    validateEPGMElementCollections(testEdges, loadedEdges);
    validateEPGMGraphElementCollections(testEdges, loadedEdges);

    epgmStore.close();
  }

  @Test
  public void testReadWithLabelRegPredicate() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    Pattern queryFormulaGraph = Pattern.compile("^Com.*");
    Pattern queryFormulaVertex = Pattern.compile("^(Per|Ta).*");
    Pattern queryFormulaEdge = Pattern.compile("^has.*");

    // Extract parts of social graph to filter for
    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .stream()
      .filter(g -> queryFormulaGraph.matcher(g.getLabel()).matches())
      .collect(Collectors.toList());

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .stream()
      .filter(e -> queryFormulaEdge.matcher(e.getLabel()).matches())
      .collect(Collectors.toList());

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .stream()
      .filter(v -> queryFormulaVertex.matcher(v.getLabel()).matches())
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore, config);

    // Apply empty graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(queryFormulaGraph))
    );

    // Apply empty edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(queryFormulaEdge))
    );

    // Apply empty vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(new HBaseLabelReg<>(queryFormulaVertex))
    );

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(testGraphs, loadedGraphHeads);
    validateEPGMElementCollections(testVertices, loadedVertices);
    validateEPGMGraphElementCollections(testVertices, loadedVertices);
    validateEPGMElementCollections(testEdges, loadedEdges);
    validateEPGMGraphElementCollections(testEdges, loadedEdges);

    epgmStore.close();
  }

  @Test
  public void testReadWithPropEqualsPredicate() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    writeSocialGraphToStore(epgmStore);

    List<PersistentGraphHead> testGraphs = new ArrayList<>(getSocialPersistentGraphHeads())
      .stream()
      .filter(it -> {
        assert it.getProperties() != null;
        return it.getProperties().get("vertexCount") != null &&
          it.getProperties()
            .get("vertexCount")
            .getInt() == 3;
      })
      .collect(Collectors.toList());

    List<PersistentEdge<Vertex>> testEdges = new ArrayList<>(getSocialPersistentEdges())
      .stream()
      .filter(it -> {
        assert it.getProperties() != null;
        return it.getProperties().get("since") != null &&
          Objects.equals(it.getProperties()
            .get("since")
            .getInt(), 2013);
      })
      .collect(Collectors.toList());

    List<PersistentVertex<Edge>> testVertices = new ArrayList<>(getSocialPersistentVertices())
      .stream()
      .filter(it -> {
        assert it.getProperties() != null;
        return it.getProperties().get("interest") != null &&
          Objects.equals(it.getProperties()
            .get("interest")
            .getString(), "Databases");
      })
      .collect(Collectors.toList());

    // Define HBase source
    HBaseDataSource hBaseDataSource = new HBaseDataSource(epgmStore, config);

    // Apply graph predicate
    hBaseDataSource = hBaseDataSource.applyGraphPredicate(
      Query.elements().fromAll().where(new HBasePropEquals<>("vertexCount", 3))
    );

    // Apply edge predicate
    hBaseDataSource = hBaseDataSource.applyEdgePredicate(
      Query.elements().fromAll().where(new HBasePropEquals<>("since", 2013))
    );

    // Apply vertex predicate
    hBaseDataSource = hBaseDataSource.applyVertexPredicate(
      Query.elements().fromAll().where(new HBasePropEquals<>("interest", "Databases"))
    );

    assertTrue(hBaseDataSource.isFilterPushedDown());

    GraphCollection graphCollection = hBaseDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads = graphCollection.getGraphHeads().collect();
    Collection<Vertex> loadedVertices = graphCollection.getVertices().collect();
    Collection<Edge> loadedEdges = graphCollection.getEdges().collect();

    validateEPGMElementCollections(testGraphs, loadedGraphHeads);
    validateEPGMElementCollections(testVertices, loadedVertices);
    validateEPGMGraphElementCollections(testVertices, loadedVertices);
    validateEPGMElementCollections(testEdges, loadedEdges);
    validateEPGMGraphElementCollections(testEdges, loadedEdges);

    epgmStore.close();
  }

  @Test
  public void testWriteToSink() throws Exception {
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    // create empty EPGM store
    HBaseEPGMStore epgmStore = createEmptyEPGMStore(getExecutionEnvironment());

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    new HBaseDataSink(epgmStore, config).write(epgmStore
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

    epgmStore.close();
  }
}
