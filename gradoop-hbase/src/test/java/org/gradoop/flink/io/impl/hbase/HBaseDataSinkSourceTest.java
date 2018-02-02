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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.EPGMDatabase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopHBaseTestBase.createEmptyEPGMStore;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.*;

public class HBaseDataSinkSourceTest extends GradoopFlinkTestBase {


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
}
