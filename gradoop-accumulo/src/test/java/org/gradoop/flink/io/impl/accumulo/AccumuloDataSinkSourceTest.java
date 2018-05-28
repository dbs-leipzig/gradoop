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

package org.gradoop.flink.io.impl.accumulo;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.AccumuloTestSuite;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.common.storage.impl.accumulo.iterator.client.CloseableIterator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.EPGMDatabase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.InputStream;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;

/**
 * accumulo data read wirte test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AccumuloDataSinkSourceTest extends GradoopFlinkTestBase {

  private static final String TEST_01 = "sink_source_test01.";
  private static final String TEST_02 = "sink_source_test02.";

  @Test
  public void test01_read() throws Exception {
    AccumuloEPGMStore<GraphHead, Vertex, Edge> accumuloStore = new AccumuloEPGMStore<>(
      AccumuloTestSuite.getAcConfig(getExecutionEnvironment(), TEST_01));

    Collection<GraphHead> graphHeads = GradoopTestUtils.getSocialNetworkLoader().getGraphHeads();
    Collection<Vertex> vertices = GradoopTestUtils.getSocialNetworkLoader().getVertices();
    Collection<Edge> edges = GradoopTestUtils.getSocialNetworkLoader().getEdges();

    // write social graph to HBase
    for (GraphHead g : graphHeads) {
      accumuloStore.writeGraphHead(g);
    }
    for (Vertex v : vertices) {
      accumuloStore.writeVertex(v);
    }
    for (Edge e : edges) {
      accumuloStore.writeEdge(e);
    }
    accumuloStore.flush();

    // read social graph from HBase via EPGMDatabase
    GraphCollection collection = new AccumuloDataSource(accumuloStore).getGraphCollection();


    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

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
  public void test02_write() throws Exception {
    AccumuloEPGMStore<GraphHead, Vertex, Edge> accumuloStore = new AccumuloEPGMStore<>(
      AccumuloTestSuite.getAcConfig(getExecutionEnvironment(), TEST_02));


    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    InputStream inputStream = getClass().getResourceAsStream(
      GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);

    EPGMDatabase epgmDB = loader.getDatabase();

    // write social graph to HBase via EPGM database
    epgmDB.writeTo(new AccumuloDataSink(accumuloStore));

    getExecutionEnvironment().execute();

    accumuloStore.flush();

    // read social network from HBase

    // graph heads
    try (
      /*graph head*/
      CloseableIterator<GraphHead> graphHeads = accumuloStore.getGraphSpace();
      /*vertices 0*/
      CloseableIterator<Vertex> vertices0 = accumuloStore.getVertexSpace();
      /*vertices 1*/
      CloseableIterator<Vertex> vertices1 = accumuloStore.getVertexSpace();
      /*edges 0*/
      CloseableIterator<Edge> edges0 = accumuloStore.getEdgeSpace();
      /*edges 1*/
      CloseableIterator<Edge> edges1 = accumuloStore.getEdgeSpace()) {
      validateEPGMElementCollections(loader.getGraphHeads(), Lists.newArrayList(graphHeads));
      validateEPGMElementCollections(loader.getVertices(), Lists.newArrayList(vertices0));
      validateEPGMGraphElementCollections(loader.getVertices(), Lists.newArrayList(vertices1));
      validateEPGMElementCollections(loader.getEdges(), Lists.newArrayList(edges0));
      validateEPGMGraphElementCollections(loader.getEdges(), Lists.newArrayList(edges1));
    }

    accumuloStore.close();
  }

}
