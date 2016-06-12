package org.gradoop.io.impl.hbase;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.impl.hbase.GradoopHBaseTestBase;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentEdges;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentGraphHeads;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestUtils.getSocialPersistentVertices;

public class HBaseIOTest extends FlinkHBaseTestBase {

  @Test
  public void readFromHBaseTest() throws Exception {
    HBaseEPGMStore<GraphHeadPojo, VertexPojo, EdgePojo> epgmStore =
      GradoopHBaseTestBase.createEmptyEPGMStore();

    List<PersistentVertex<EdgePojo>> vertices =
      Lists.newArrayList(getSocialPersistentVertices());
    List<PersistentEdge<VertexPojo>> edges =
      Lists.newArrayList(getSocialPersistentEdges());
    List<PersistentGraphHead> graphHeads =
      Lists.newArrayList(getSocialPersistentGraphHeads());

    // write social graph to HBase
    for (PersistentGraphHead g : graphHeads) {
      epgmStore.writeGraphHead(g);
    }
    for (PersistentVertex<EdgePojo> v : vertices) {
      epgmStore.writeVertex(v);
    }
    for (PersistentEdge<VertexPojo> e : edges) {
      epgmStore.writeEdge(e);
    }

    epgmStore.flush();

    // read social graph from HBase via EPGMDatabase
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      new HBaseDataSource<>(epgmStore, getConfig()).getGraphCollection();

    Collection<GraphHeadPojo> loadedGraphHeads    = Lists.newArrayList();
    Collection<VertexPojo>    loadedVertices      = Lists.newArrayList();
    Collection<EdgePojo>      loadedEdges         = Lists.newArrayList();

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
    HBaseEPGMStore<GraphHeadPojo, VertexPojo, EdgePojo> epgmStore =
      GradoopHBaseTestBase.createEmptyEPGMStore();

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDB =
      loader.getDatabase();

    // write social graph to HBase via EPGM database
    epgmDB.writeTo(new HBaseDataSink<>(epgmStore, getConfig()));

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