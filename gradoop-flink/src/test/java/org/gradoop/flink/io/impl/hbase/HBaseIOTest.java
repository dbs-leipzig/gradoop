package org.gradoop.flink.io.impl.hbase;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.EPGMDatabase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.GradoopHBaseTestBase;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentEdges;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentGraphHeads;
import static org.gradoop.common.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentVertices;

public class HBaseIOTest extends FlinkHBaseTestBase {

  @Test
  public void readFromHBaseTest() throws Exception {
    HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore =
      GradoopHBaseTestBase.createEmptyEPGMStore();

    List<PersistentVertex<Edge>> vertices =
      Lists.newArrayList(getSocialPersistentVertices());
    List<PersistentEdge<Vertex>> edges =
      Lists.newArrayList(getSocialPersistentEdges());
    List<PersistentGraphHead> graphHeads =
      Lists.newArrayList(getSocialPersistentGraphHeads());

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
    GraphCollection collection = new HBaseDataSource(epgmStore,
      getConfig()).getGraphCollection();

    Collection<GraphHead> loadedGraphHeads    = Lists.newArrayList();
    Collection<Vertex>    loadedVertices      = Lists.newArrayList();
    Collection<Edge>      loadedEdges         = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(edges, loadedEdges);

    epgmStore.close();
  }

  @Test
  public void writeToHBaseTest() throws Exception {
    // create empty EPGM store
    HBaseEPGMStore<GraphHead, Vertex, Edge> epgmStore =
      GradoopHBaseTestBase.createEmptyEPGMStore();

    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    EPGMDatabase epgmDB = loader.getDatabase();

    // write social graph to HBase via EPGM database
    epgmDB.writeTo(new HBaseDataSink(epgmStore, getConfig()));

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