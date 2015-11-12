package org.gradoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.gradoop.GradoopTestBaseUtils.*;
import static org.junit.Assert.*;

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

  public static EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> createEmptyEPGMStore() {
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

  /**
   * Copies the given local file into HDFS.
   *
   * @param inputFile path to local file
   * @throws IOException
   */
  protected void copyFromLocal(String inputFile) throws IOException {
    URL tmpUrl =
      Thread.currentThread().getContextClassLoader().getResource(inputFile);
    assertNotNull(tmpUrl);
    String graphFileResource = tmpUrl.getPath();
    // copy input graph to DFS
    FileSystem fs = utility.getTestFileSystem();
    Path graphFileLocalPath = new Path(graphFileResource);
    Path graphFileDFSPath = new Path(inputFile);
    fs.copyFromLocalFile(graphFileLocalPath, graphFileDFSPath);
  }

  /**
   * Creates a HBase table with the given name.
   *
   * @param outputTable table name
   * @throws IOException
   */
  protected void createTable(String outputTable) throws IOException {
    HTableDescriptor outputTableDescriptor =
      new HTableDescriptor(TableName.valueOf(outputTable));

    HBaseAdmin admin = new HBaseAdmin(utility.getConfiguration());

    if (!admin.tableExists(outputTableDescriptor.getName())) {
      outputTableDescriptor.addFamily(new HColumnDescriptor("v"));
      admin.createTable(outputTableDescriptor);
    }

    admin.close();
  }

  /**
   * Reads a graph file in HDFS line by line into an array and returns it.
   *
   * @param graphFileName file in HDFS
   * @param lineCount     number of lines
   * @return array with line contents
   * @throws IOException
   */
  protected String[] readGraphFromFile(final Path graphFileName,
    final int lineCount) throws IOException {
    BufferedReader br = new BufferedReader(
      new InputStreamReader(utility.getTestFileSystem().open(graphFileName)));
    String line;
    int i = 0;
    String[] fileContent = new String[lineCount];
    while ((line = br.readLine()) != null) {
      fileContent[i] = line;
      i++;
    }
    return fileContent;
  }

  public static Collection<PersistentVertex<EdgePojo>>
  createPersistentSocialVertices() {
    Collection<VertexPojo> vertexDataCollection =
      createVertexPojoCollection();
    List<PersistentVertex<EdgePojo>> persistentVertexData =
      Lists.newArrayListWithExpectedSize(vertexDataCollection.size());
    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      vertexDataFactory = new HBaseVertexFactory();


    Set<EdgePojo> outEdges = null;
    Set<EdgePojo> inEdges = null;
    for (VertexPojo vertexData : vertexDataCollection) {
      if (vertexData.getId().equals(VERTEX_PERSON_ALICE.getId())) {
        outEdges = Sets.newHashSet(EDGE_0_KNOWS, EDGE_8_HAS_INTEREST);
        inEdges = Sets
          .newHashSet(EDGE_1_KNOWS, EDGE_6_KNOWS, EDGE_15_HAS_MODERATOR,
            EDGE_17_HAS_MEMBER);

      } else if (vertexData.getId().equals(VERTEX_PERSON_BOB.getId())) {
        outEdges = Sets.newHashSet(EDGE_1_KNOWS, EDGE_2_KNOWS);
        inEdges = Sets
          .newHashSet(EDGE_0_KNOWS, EDGE_3_KNOWS, EDGE_18_HAS_MEMBER,
            EDGE_21_KNOWS);
      } else if (vertexData.getId().equals(VERTEX_PERSON_CAROL.getId())) {
        outEdges = Sets.newHashSet(EDGE_3_KNOWS, EDGE_4_KNOWS);
        inEdges = Sets
          .newHashSet(EDGE_2_KNOWS, EDGE_5_KNOWS, EDGE_19_HAS_MEMBER,
            EDGE_22_KNOWS);
      } else if (vertexData.getId().equals(VERTEX_PERSON_DAVE.getId())) {
        outEdges = Sets.newHashSet(EDGE_5_KNOWS, EDGE_9_HAS_INTEREST);
        inEdges = Sets
          .newHashSet(EDGE_4_KNOWS, EDGE_16_HAS_MODERATOR, EDGE_20_HAS_MEMBER,
            EDGE_23_KNOWS);
      } else if (vertexData.getId().equals(VERTEX_PERSON_EVE.getId())) {
        outEdges =
          Sets.newHashSet(EDGE_6_KNOWS, EDGE_7_HAS_INTEREST, EDGE_21_KNOWS);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(VERTEX_PERSON_FRANK.getId())) {
        outEdges =
          Sets.newHashSet(EDGE_10_HAS_INTEREST, EDGE_22_KNOWS, EDGE_23_KNOWS);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(VERTEX_TAG_DATABASES.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(EDGE_7_HAS_INTEREST, EDGE_8_HAS_INTEREST,
          EDGE_11_HAS_TAG);
      } else if (vertexData.getId().equals(VERTEX_TAG_GRAPHS.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(EDGE_12_HAS_TAG, EDGE_13_HAS_TAG);
      } else if (vertexData.getId().equals(VERTEX_TAG_HADOOP.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(EDGE_9_HAS_INTEREST, EDGE_10_HAS_INTEREST,
          EDGE_14_HAS_TAG);
      } else if (vertexData.getId().equals(VERTEX_FORUM_GDBS.getId())) {
        outEdges = Sets
          .newHashSet(EDGE_11_HAS_TAG, EDGE_12_HAS_TAG, EDGE_15_HAS_MODERATOR,
            EDGE_17_HAS_MEMBER, EDGE_18_HAS_MEMBER);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(VERTEX_FORUM_GPS.getId())) {
        outEdges = Sets
          .newHashSet(EDGE_13_HAS_TAG, EDGE_14_HAS_TAG, EDGE_16_HAS_MODERATOR,
            EDGE_19_HAS_MEMBER, EDGE_20_HAS_MEMBER);
        inEdges = Sets.newHashSet();
      }
      persistentVertexData
        .add(vertexDataFactory.createVertex(vertexData, outEdges, inEdges));
    }
    return persistentVertexData;
  }

  public static Collection<PersistentEdge<VertexPojo>>
  createPersistentSocialEdges() {
    Collection<EdgePojo> edgeDataCollection = createEdgePojoCollection();
    List<PersistentEdge<VertexPojo>> persistentEdgeData =
      Lists.newArrayListWithExpectedSize(edgeDataCollection.size());
    PersistentEdgeFactory<EdgePojo, VertexPojo, HBaseEdge>
      edgeDataFactory = new HBaseEdgeFactory();

    VertexPojo sourceVertexData = null;
    VertexPojo targetVertexData = null;
    for (EdgePojo edgeData : edgeDataCollection) {
      if (edgeData.getId().equals(EDGE_0_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_ALICE;
        targetVertexData = VERTEX_PERSON_BOB;
      } else if (edgeData.getId().equals(EDGE_1_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_BOB;
        targetVertexData = VERTEX_PERSON_ALICE;
      } else if (edgeData.getId().equals(EDGE_2_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_BOB;
        targetVertexData = VERTEX_PERSON_CAROL;
      } else if (edgeData.getId().equals(EDGE_3_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_CAROL;
        targetVertexData = VERTEX_PERSON_BOB;
      } else if (edgeData.getId().equals(EDGE_4_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_CAROL;
        targetVertexData = VERTEX_PERSON_DAVE;
      } else if (edgeData.getId().equals(EDGE_5_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_DAVE;
        targetVertexData = VERTEX_PERSON_CAROL;
      } else if (edgeData.getId().equals(EDGE_6_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_EVE;
        targetVertexData = VERTEX_PERSON_ALICE;
      } else if (edgeData.getId().equals(EDGE_7_HAS_INTEREST.getId())) {
        sourceVertexData = VERTEX_PERSON_EVE;
        targetVertexData = VERTEX_TAG_DATABASES;
      } else if (edgeData.getId().equals(EDGE_8_HAS_INTEREST.getId())) {
        sourceVertexData = VERTEX_PERSON_ALICE;
        targetVertexData = VERTEX_TAG_DATABASES;
      } else if (edgeData.getId().equals(EDGE_9_HAS_INTEREST.getId())) {
        sourceVertexData = VERTEX_PERSON_DAVE;
        targetVertexData = VERTEX_TAG_HADOOP;
      } else if (edgeData.getId().equals(EDGE_10_HAS_INTEREST.getId())) {
        sourceVertexData = VERTEX_PERSON_FRANK;
        targetVertexData = VERTEX_TAG_HADOOP;
      } else if (edgeData.getId().equals(EDGE_11_HAS_TAG.getId())) {
        sourceVertexData = VERTEX_FORUM_GDBS;
        targetVertexData = VERTEX_TAG_DATABASES;
      } else if (edgeData.getId().equals(EDGE_12_HAS_TAG.getId())) {
        sourceVertexData = VERTEX_FORUM_GDBS;
        targetVertexData = VERTEX_TAG_GRAPHS;
      } else if (edgeData.getId().equals(EDGE_13_HAS_TAG.getId())) {
        sourceVertexData = VERTEX_FORUM_GPS;
        targetVertexData = VERTEX_TAG_GRAPHS;
      } else if (edgeData.getId().equals(EDGE_14_HAS_TAG.getId())) {
        sourceVertexData = VERTEX_FORUM_GPS;
        targetVertexData = VERTEX_TAG_HADOOP;
      } else if (edgeData.getId().equals(EDGE_15_HAS_MODERATOR.getId())) {
        sourceVertexData = VERTEX_FORUM_GDBS;
        targetVertexData = VERTEX_PERSON_ALICE;
      } else if (edgeData.getId().equals(EDGE_16_HAS_MODERATOR.getId())) {
        sourceVertexData = VERTEX_FORUM_GPS;
        targetVertexData = VERTEX_PERSON_DAVE;
      } else if (edgeData.getId().equals(EDGE_17_HAS_MEMBER.getId())) {
        sourceVertexData = VERTEX_FORUM_GDBS;
        targetVertexData = VERTEX_PERSON_ALICE;
      } else if (edgeData.getId().equals(EDGE_18_HAS_MEMBER.getId())) {
        sourceVertexData = VERTEX_FORUM_GDBS;
        targetVertexData = VERTEX_PERSON_BOB;
      } else if (edgeData.getId().equals(EDGE_19_HAS_MEMBER.getId())) {
        sourceVertexData = VERTEX_FORUM_GPS;
        targetVertexData = VERTEX_PERSON_CAROL;
      } else if (edgeData.getId().equals(EDGE_20_HAS_MEMBER.getId())) {
        sourceVertexData = VERTEX_FORUM_GPS;
        targetVertexData = VERTEX_PERSON_DAVE;
      } else if (edgeData.getId().equals(EDGE_21_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_EVE;
        targetVertexData = VERTEX_PERSON_BOB;
      } else if (edgeData.getId().equals(EDGE_22_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_FRANK;
        targetVertexData = VERTEX_PERSON_CAROL;
      } else if (edgeData.getId().equals(EDGE_23_KNOWS.getId())) {
        sourceVertexData = VERTEX_PERSON_FRANK;
        targetVertexData = VERTEX_PERSON_DAVE;
      }
      persistentEdgeData.add(edgeDataFactory
        .createEdge(edgeData, sourceVertexData, targetVertexData));
    }

    return persistentEdgeData;
  }

  public static Collection<PersistentGraphHead>
  createPersistentSocialGraphHead() {
    Collection<GraphHeadPojo> graphDataCollection =
      createGraphHeadCollection();
    List<PersistentGraphHead> persistentGraphData =
      Lists.newArrayListWithExpectedSize(graphDataCollection.size());
    PersistentGraphHeadFactory<GraphHeadPojo, HBaseGraphHead>
      graphDataFactory = new HBaseGraphHeadFactory();

    GradoopIds vertexIds = null;
    GradoopIds edgeIds = null;

    for (GraphHeadPojo graphData : graphDataCollection) {
      if (graphData.getId().equals(communityDatabases.getId())) {
        vertexIds = new GradoopIds(VERTEX_PERSON_ALICE.getId(),
          VERTEX_PERSON_BOB.getId(), VERTEX_PERSON_EVE.getId());
        edgeIds = new GradoopIds(EDGE_0_KNOWS.getId(), EDGE_1_KNOWS.getId(), EDGE_6_KNOWS

            .getId(), EDGE_21_KNOWS.getId());
      } else if (graphData.getId().equals(communityHadoop.getId())) {
        vertexIds =
          new GradoopIds(VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_DAVE

            .getId(), VERTEX_PERSON_FRANK

            .getId());
        edgeIds = new GradoopIds(EDGE_4_KNOWS.getId(), EDGE_5_KNOWS.getId(), EDGE_22_KNOWS

            .getId(), EDGE_23_KNOWS.getId());
      } else if (graphData.getId().equals(communityGraphs.getId())) {
        vertexIds = new GradoopIds(VERTEX_PERSON_ALICE.getId(), VERTEX_PERSON_BOB.getId(),
            VERTEX_PERSON_CAROL

              .getId(), VERTEX_PERSON_DAVE

              .getId());
        edgeIds = new GradoopIds(EDGE_0_KNOWS.getId(), EDGE_1_KNOWS.getId(), EDGE_2_KNOWS

              .getId(), EDGE_3_KNOWS.getId(), EDGE_4_KNOWS.getId(),
            EDGE_5_KNOWS.getId());
      } else if (graphData.getId().equals(forumGraph.getId())) {
        vertexIds = new GradoopIds(VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_DAVE.getId(),
            VERTEX_PERSON_FRANK.getId(), VERTEX_FORUM_GPS.getId());
        edgeIds = new GradoopIds(EDGE_4_KNOWS.getId(), EDGE_16_HAS_MODERATOR.getId(),
            EDGE_19_HAS_MEMBER.getId(), EDGE_20_HAS_MEMBER.getId());
      }
      persistentGraphData
        .add(graphDataFactory.createGraphHead(graphData, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  public static Iterable<PersistentGraphHead> createPersistentGraphHead() {
    List<PersistentGraphHead> persistentGraphData =
      Lists.newArrayListWithExpectedSize(2);

    EPGMGraphHeadFactory<GraphHeadPojo> graphHeadFactory =
      new GraphHeadPojoFactory();
    // graph 0
    GradoopId graphID = GradoopId.createId(0L);
    String graphLabel = "A";
    Map<String, Object> graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    graphProperties.put("k2", "v2");

    GradoopIds vertices = GradoopIds.createGradoopIds(0L, 1L);
    GradoopIds edges = GradoopIds.createGradoopIds(2L, 3L);

    persistentGraphData.add(
      new HBaseGraphHead(graphHeadFactory.createGraphHead(
        graphID, graphLabel, graphProperties), vertices, edges));

    // graph 1
    graphID = GradoopId.createId(1L);
    graphLabel = "A";
    graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    vertices = GradoopIds.createGradoopIds(1L, 2L);
    edges = GradoopIds.createGradoopIds(4L, 5L);

    persistentGraphData.add(new HBaseGraphHead(
      graphHeadFactory.createGraphHead(graphID, graphLabel, graphProperties),
      vertices, edges));

    return persistentGraphData;
  }

  public static Iterable<PersistentVertex<EdgePojo>> createPersistentVertex() {
    List<PersistentVertex<EdgePojo>> persistentVertexData =
      Lists.newArrayListWithExpectedSize(2);

    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      persistentVertexFactory = new HBaseVertexFactory();
    EPGMVertexFactory<VertexPojo> vertexFactory =
      new VertexPojoFactory();
    EPGMEdgeFactory<EdgePojo> edgeFactory =
      new EdgePojoFactory();
    // vertex 0
    GradoopId vertexId = GradoopId.createId(0L);
    String vertexLabel = "A";
    Map<String, Object> vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    vertexProperties.put("k2", "v2");
    GradoopIds graphs = GradoopIds.createGradoopIds(0L, 1L);
    Set<EdgePojo> outgoingEdgeData = Sets.newHashSetWithExpectedSize(2);
    outgoingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(0L), "a",
      GradoopId.createId(0L), GradoopId.createId(1L)));
    outgoingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(1L), "b",
      GradoopId.createId(0L), GradoopId.createId(2L)));
    Set<EdgePojo> incomingEdgeData = Sets.newHashSetWithExpectedSize(2);
    incomingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(2L), "a",
      GradoopId.createId(1L), GradoopId.createId(0L)));
    incomingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(3L), "c",
      GradoopId.createId(2L), GradoopId.createId(0L)));

    persistentVertexData.add(persistentVertexFactory.createVertex(vertexFactory
        .createVertex(vertexId, vertexLabel, vertexProperties, graphs),
      outgoingEdgeData, incomingEdgeData));

    // vertex 1
    vertexId = GradoopId.createId(1L);
    vertexLabel = "B";
    vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    graphs = GradoopIds.createGradoopIds(1L, 2L);
    outgoingEdgeData = Sets.newHashSetWithExpectedSize(2);
    outgoingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(2L),
      GradoopId.createId(1L), GradoopId.createId(0L)));
    outgoingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(4L),
      GradoopId.createId(1L), GradoopId.createId(2L)));
    incomingEdgeData = Sets.newHashSetWithExpectedSize(2);
    incomingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(0L),
      GradoopId.createId(0L), GradoopId.createId(1L)));
    incomingEdgeData.add(edgeFactory.createEdge(GradoopId.createId(5L),
      GradoopId.createId(2L), GradoopId.createId(1L)));

    persistentVertexData.add(persistentVertexFactory.createVertex(vertexFactory
        .createVertex(vertexId, vertexLabel, vertexProperties, graphs),
      outgoingEdgeData, incomingEdgeData));

    return persistentVertexData;
  }

  public static Iterable<PersistentEdge<VertexPojo>> createPersistentEdge() {
    List<PersistentEdge<VertexPojo>> persistentEdgeData =
      Lists.newArrayListWithExpectedSize(2);

    PersistentEdgeFactory<EdgePojo, VertexPojo, HBaseEdge>
      persistentEdgeFactory = new HBaseEdgeFactory();
    EPGMVertexFactory<VertexPojo> vertexFactory =
      new VertexPojoFactory();
    EPGMEdgeFactory<EdgePojo> edgeFactory =
      new EdgePojoFactory();
    // edge 0
    GradoopId edgeId = GradoopId.createId(0L);
    String edgeLabel = "a";
    Map<String, Object> edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    edgeProperties.put("k2", "v2");
    GradoopIds graphs = GradoopIds.createGradoopIds(0L, 1L);
    VertexPojo edgeSourceData =
      vertexFactory.createVertex(GradoopId.createId(0L), "A");
    VertexPojo edgeTargetData =
      vertexFactory.createVertex(GradoopId.createId(1L), "B");

    persistentEdgeData.add(persistentEdgeFactory.createEdge(edgeFactory
        .createEdge(edgeId, edgeLabel, edgeSourceData.getId(),
          edgeTargetData.getId(), edgeProperties, graphs), edgeSourceData,
      edgeTargetData));

    // edge 1
    edgeId = GradoopId.createId(1L);
    edgeLabel = "b";
    edgeProperties = Maps.newHashMapWithExpectedSize(1);
    edgeProperties.put("k1", "v1");
    graphs = GradoopIds.createGradoopIds(1L, 2L);
    edgeSourceData = vertexFactory.createVertex(GradoopId.createId(0L), "A");
    edgeTargetData = vertexFactory.createVertex(GradoopId.createId(2L), "C");

    persistentEdgeData.add(persistentEdgeFactory.createEdge(edgeFactory
        .createEdge(edgeId, edgeLabel, edgeSourceData.getId(),
          edgeTargetData.getId(), edgeProperties, graphs), edgeSourceData,
      edgeTargetData));

    return persistentEdgeData;
  }

  /**
   * Checks data consistency with {@code createPersistentGraphData()}.
   *
   * @param graphStore graph store
   */
  public static void validateGraphHead(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore) {
    // g0
    EPGMGraphHead g = graphStore.readGraph(GradoopId.createId(0L));
    assertNotNull(g);
    assertEquals("A", g.getLabel());
    List<String> propertyKeys = Lists.newArrayList(g.getPropertyKeys());
    assertEquals(2, propertyKeys.size());
    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", g.getProperty("k1"));
      } else if (key.equals("v2")) {
        assertEquals("v2", g.getProperty("k2"));
      }
    }

    // g1
    g = graphStore.readGraph(GradoopId.createId(1L));
    assertNotNull(g);
    assertEquals("A", g.getLabel());
    propertyKeys = Lists.newArrayList(g.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", g.getProperty("k1"));
  }

  /**
   * Checks data consistency with {@code createPersistentVertexData()}.
   *
   * @param graphStore graph store
   */
  public static void validateVertex(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore) {
    // vertex 0
    EPGMVertex v = graphStore.readVertex(GradoopId.createId(0L));
    assertNotNull(v);
    assertEquals("A", v.getLabel());
    List<String> propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(2, propertyKeys.size());
    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", v.getProperty("k1"));
      } else if (key.equals("v2")) {
        assertEquals("v2", v.getProperty("k2"));
      }
    }
    assertEquals(2, v.getGraphCount());
    assertTrue(v.getGraphIds().contains(GradoopId.createId(0L)));
    assertTrue(v.getGraphIds().contains(GradoopId.createId(1L)));

    // vertex 1
    v = graphStore.readVertex(GradoopId.createId(1L));
    assertNotNull(v);
    assertEquals("B", v.getLabel());
    propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", v.getProperty("k1"));
    assertEquals(2, v.getGraphCount());
    assertTrue(v.getGraphIds().contains(GradoopId.createId(1L)));
    assertTrue(v.getGraphIds().contains(GradoopId.createId(2L)));
  }

  /**
   * Checks data consistency with {@code createPersistentEdgeData()}.
   *
   * @param graphStore graph store
   */
  public static void validateEdge(
    EPGMStore<VertexPojo, EdgePojo, GraphHeadPojo> graphStore) {
    // edge 0
    EPGMEdge e = graphStore.readEdge(GradoopId.createId(0L));
    assertNotNull(e);
    assertEquals("a", e.getLabel());
    assertEquals(new Long(0L), e.getSourceVertexId());
    assertEquals(new Long(1L), e.getTargetVertexId());
    assertEquals(2L, e.getPropertyCount());
    List<String> propertyKeys = Lists.newArrayList(e.getPropertyKeys());
    assertEquals(2, propertyKeys.size());
    for (String key : propertyKeys) {
      if (key.equals("k1")) {
        assertEquals("v1", e.getProperty("k1"));
      } else if (key.equals("v2")) {
        assertEquals("v2", e.getProperty("k2"));
      }
    }
    assertEquals(2, e.getGraphCount());
    assertTrue(e.getGraphIds().contains(GradoopId.createId(0L)));
    assertTrue(e.getGraphIds().contains(GradoopId.createId(1L)));

    // edge 1
    e = graphStore.readEdge(GradoopId.createId(1L));
    assertNotNull(e);
    assertEquals("b", e.getLabel());
    assertEquals(GradoopId.createId(0L), e.getSourceVertexId());
    assertEquals(GradoopId.createId(2L), e.getTargetVertexId());
    assertEquals(1L, e.getPropertyCount());
    propertyKeys = Lists.newArrayList(e.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", e.getProperty("k1"));
    assertEquals(2, e.getGraphCount());
    assertTrue(e.getGraphIds().contains(GradoopId.createId(1L)));
    assertTrue(e.getGraphIds().contains(GradoopId.createId(2L)));

  }
}
