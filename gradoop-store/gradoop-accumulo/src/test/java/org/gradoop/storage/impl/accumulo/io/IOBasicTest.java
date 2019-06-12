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
package org.gradoop.storage.impl.accumulo.io;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.AccumuloTestSuite;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;

/**
 * accumulo data read write test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOBasicTest extends GradoopFlinkTestBase {

  private static final String TEST_01 = "io_basic_01";
  private static final String TEST_02 = "io_basic_02";

  @Test
  public void read() throws Exception {
    AccumuloEPGMStore accumuloStore = new AccumuloEPGMStore(AccumuloTestSuite.getAcConfig(TEST_01));

    Collection<EPGMGraphHead> graphHeads = GradoopTestUtils.getSocialNetworkLoader().getGraphHeads();
    Collection<EPGMVertex> vertices = GradoopTestUtils.getSocialNetworkLoader().getVertices();
    Collection<EPGMEdge> edges = GradoopTestUtils.getSocialNetworkLoader().getEdges();

    // write social graph to HBase
    for (EPGMGraphHead g : graphHeads) {
      accumuloStore.writeGraphHead(g);
    }
    for (EPGMVertex v : vertices) {
      accumuloStore.writeVertex(v);
    }
    for (EPGMEdge e : edges) {
      accumuloStore.writeEdge(e);
    }
    accumuloStore.flush();

    GradoopFlinkConfig flinkConfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    GraphCollection collection = new AccumuloDataSource(accumuloStore, flinkConfig)
      .getGraphCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);

    accumuloStore.close();
  }

  @Test
  public void write() throws Exception {
    AccumuloEPGMStore accumuloStore = new AccumuloEPGMStore(AccumuloTestSuite.getAcConfig(TEST_02));

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    InputStream inputStream = getClass().getResourceAsStream(
      GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    GradoopFlinkConfig flinkConfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    new AccumuloDataSink(accumuloStore, flinkConfig)
      .write(flinkConfig.getGraphCollectionFactory()
        .fromCollections(
          loader.getGraphHeads(),
          loader.getVertices(),
          loader.getEdges()));
    getExecutionEnvironment().execute();
    accumuloStore.flush();

    validateEPGMElementCollections(loader.getGraphHeads(),
      accumuloStore.getGraphSpace().readRemainsAndClose());
    validateEPGMElementCollections(loader.getVertices(),
      accumuloStore.getVertexSpace().readRemainsAndClose());
    validateEPGMGraphElementCollections(loader.getVertices(),
      accumuloStore.getVertexSpace().readRemainsAndClose());
    validateEPGMElementCollections(loader.getEdges(),
      accumuloStore.getEdgeSpace().readRemainsAndClose());
    validateEPGMGraphElementCollections(loader.getEdges(),
      accumuloStore.getEdgeSpace().readRemainsAndClose());

    accumuloStore.close();
  }

}
