package org.gradoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.impl.hbase.HBaseEdge;
import org.gradoop.storage.impl.hbase.HBaseEdgeFactory;
import org.gradoop.storage.impl.hbase.HBaseGraphHead;
import org.gradoop.storage.impl.hbase.HBaseGraphHeadFactory;
import org.gradoop.storage.impl.hbase.HBaseVertex;
import org.gradoop.storage.impl.hbase.HBaseVertexFactory;
import org.gradoop.storage.impl.hbase.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStoreFactory;
import org.gradoop.util.AsciiGraphLoader;
import org.gradoop.util.GradoopConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used for tests that need a HBase cluster to run.
 */
public class HBaseTestBase {

  protected static HBaseTestingUtility utility;

  /**
   * Starts the mini cluster for all tests.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUp() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility(HBaseConfiguration.create());
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }
  }

  /**
   * Stops the test cluster after the test.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }

  public static EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo>
  createEmptyEPGMStore() {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig());
  }

  /**
   * Open EPGMStore for test purposes.
   *
   * @return EPGMStore with vertices and edges
   */
  public static EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> openEPGMStore() {
    Configuration config = utility.getConfiguration();

    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig());
  }


  private static AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
  getSocialNetworkLoader() throws
    IOException {
    GradoopConfig<GraphHeadPojo, VertexPojo, EdgePojo> config = GradoopConfig
      .getDefaultConfig();

    return AsciiGraphLoader.fromFile("/data/social_network.gdl", config);
  }

  // PersistentGraphHead

  public static Collection<PersistentGraphHead>
  getSocialPersistentGraphHeads() throws IOException {
    return getPersistentGraphHeads(getSocialNetworkLoader());
  }

  private static Collection<PersistentGraphHead> getPersistentGraphHeads(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {

    PersistentGraphHeadFactory<GraphHeadPojo, HBaseGraphHead>
      graphDataFactory = new HBaseGraphHeadFactory();

    List<PersistentGraphHead> persistentGraphData = new ArrayList<>();

    for(GraphHeadPojo graphHead : loader.getGraphHeads()) {

      GradoopId graphId = graphHead.getId();
      GradoopIdSet vertexIds = new GradoopIdSet();
      GradoopIdSet edgeIds = new GradoopIdSet();

      for (VertexPojo vertex : loader.getVertices()) {
        if (vertex.getGraphIds().contains(graphId)) {
          vertexIds.add(vertex.getId());
        }
      }
      for (EdgePojo edge : loader.getEdges()) {
        if (edge.getGraphIds().contains(graphId)) {
          edgeIds.add(edge.getId());
        }
      }

      persistentGraphData.add(
        graphDataFactory.createGraphHead(graphHead, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  // PersistentVertex

  public static Collection<PersistentVertex<EdgePojo>>
  getSocialPersistentVertices() throws IOException {
    return getPersistentVertices(getSocialNetworkLoader());
  }

  private static List<PersistentVertex<EdgePojo>> getPersistentVertices(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {
    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      vertexDataFactory = new HBaseVertexFactory();

    List<PersistentVertex<EdgePojo>> persistentVertexData = new ArrayList<>();

    for(VertexPojo vertex : loader.getVertices()) {

      Set<EdgePojo> outEdges = new HashSet<>();
      Set<EdgePojo> inEdges = new HashSet<>();

      for(EdgePojo edge : loader.getEdges()) {
        if(edge.getSourceVertexId().equals(vertex.getId())) {
          outEdges.add(edge);
        }
        if(edge.getTargetVertexId().equals(vertex.getId())) {
          inEdges.add(edge);
        }
      }
      persistentVertexData.add(
        vertexDataFactory.createVertex(vertex, outEdges, inEdges));
    } return persistentVertexData;
  }

  // PersistentEdge

  public static Collection<PersistentEdge<VertexPojo>>
  getSocialPersistentEdges() throws IOException {
    return getPersistentEdges(getSocialNetworkLoader());
  }

  private static List<PersistentEdge<VertexPojo>> getPersistentEdges(
    AsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader) {
    PersistentEdgeFactory<EdgePojo, VertexPojo, HBaseEdge>
      edgeDataFactory = new HBaseEdgeFactory();

    List<PersistentEdge<VertexPojo>> persistentEdgeData = new ArrayList<>();

    Map<GradoopId, VertexPojo> vertexById = new HashMap<>();

    for(VertexPojo vertex : loader.getVertices()) {
      vertexById.put(vertex.getId(), vertex);
    }

    for(EdgePojo edge : loader.getEdges()) {
      persistentEdgeData.add(
        edgeDataFactory.createEdge(
          edge,
          vertexById.get(edge.getSourceVertexId()),
          vertexById.get(edge.getTargetVertexId())
        )
      );
    }
    return persistentEdgeData;
  }


}
