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
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultEdgeDataFactory;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultGraphDataFactory;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.pojo.DefaultVertexDataFactory;
import org.gradoop.storage.api.*;
import org.gradoop.storage.impl.hbase.*;
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

  public static EPGMStore<DefaultVertexData, DefaultEdgeData,
      DefaultGraphData> createEmptyEPGMStore() {
    Configuration config = utility.getConfiguration();
    VertexDataHandler<DefaultVertexData, DefaultEdgeData> vertexDataHandler =
      new DefaultVertexDataHandler<>(new DefaultVertexDataFactory());
    EdgeDataHandler<DefaultEdgeData, DefaultVertexData> edgeDataHandler =
      new DefaultEdgeDataHandler<>(new DefaultEdgeDataFactory());
    GraphDataHandler<DefaultGraphData> graphDataHandler =
      new DefaultGraphDataHandler<>(new DefaultGraphDataFactory());

    HBaseEPGMStoreFactory.deleteEPGMStore(config);
    return HBaseEPGMStoreFactory
      .createOrOpenEPGMStore(config, vertexDataHandler, edgeDataHandler,
        graphDataHandler);
  }

  /**
   * Open EPGMStore for test purposes.
   *
   * @return EPGMStore with vertices and edges
   */
  public static EPGMStore<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> openEPGMStore() {
    Configuration config = utility.getConfiguration();
    VertexDataHandler<DefaultVertexData, DefaultEdgeData> vertexDataHandler =
      new DefaultVertexDataHandler<>(new DefaultVertexDataFactory());
    EdgeDataHandler<DefaultEdgeData, DefaultVertexData> edgeDataHandler =
      new DefaultEdgeDataHandler<>(new DefaultEdgeDataFactory());
    GraphDataHandler<DefaultGraphData> graphDataHandler =
      new DefaultGraphDataHandler<>(new DefaultGraphDataFactory());
    return HBaseEPGMStoreFactory
      .createOrOpenEPGMStore(config, vertexDataHandler, edgeDataHandler,
        graphDataHandler);
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

  public static Collection<PersistentVertexData<DefaultEdgeData>>
  createPersistentSocialVertexData() {
    Collection<DefaultVertexData> vertexDataCollection =
      createVertexDataCollection();
    List<PersistentVertexData<DefaultEdgeData>> persistentVertexData =
      Lists.newArrayListWithExpectedSize(vertexDataCollection.size());
    PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData, DefaultPersistentVertexData>
      vertexDataFactory = new DefaultPersistentVertexDataFactory();


    Set<DefaultEdgeData> outEdges = null;
    Set<DefaultEdgeData> inEdges = null;
    for (DefaultVertexData vertexData : vertexDataCollection) {
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
        .add(vertexDataFactory.createVertexData(vertexData, outEdges, inEdges));
    }
    return persistentVertexData;
  }

  public static Collection<PersistentEdgeData<DefaultVertexData>>
  createPersistentSocialEdgeData() {
    Collection<DefaultEdgeData> edgeDataCollection = createEdgeDataCollection();
    List<PersistentEdgeData<DefaultVertexData>> persistentEdgeData =
      Lists.newArrayListWithExpectedSize(edgeDataCollection.size());
    PersistentEdgeDataFactory<DefaultEdgeData, DefaultVertexData, DefaultPersistentEdgeData>
      edgeDataFactory = new DefaultPersistentEdgeDataFactory();

    DefaultVertexData sourceVertexData = null;
    DefaultVertexData targetVertexData = null;
    for (DefaultEdgeData edgeData : edgeDataCollection) {
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
        .createEdgeData(edgeData, sourceVertexData, targetVertexData));
    }

    return persistentEdgeData;
  }

  public static Collection<PersistentGraphData>
  createPersistentSocialGraphData() {
    Collection<DefaultGraphData> graphDataCollection =
      createGraphDataCollection();
    List<PersistentGraphData> persistentGraphData =
      Lists.newArrayListWithExpectedSize(graphDataCollection.size());
    PersistentGraphDataFactory<DefaultGraphData, DefaultPersistentGraphData>
      graphDataFactory = new DefaultPersistentGraphDataFactory();

    Set<Long> vertexIds = null;
    Set<Long> edgeIds = null;

    for (DefaultGraphData graphData : graphDataCollection) {
      if (graphData.getId().equals(communityDatabases.getId())) {
        vertexIds =
          Sets.newHashSet(VERTEX_PERSON_ALICE.getId(), VERTEX_PERSON_BOB

            .getId(), VERTEX_PERSON_EVE

            .getId());
        edgeIds = Sets
          .newHashSet(EDGE_0_KNOWS.getId(), EDGE_1_KNOWS.getId(), EDGE_6_KNOWS

            .getId(), EDGE_21_KNOWS.getId());
      } else if (graphData.getId().equals(communityHadoop.getId())) {
        vertexIds =
          Sets.newHashSet(VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_DAVE

            .getId(), VERTEX_PERSON_FRANK

            .getId());
        edgeIds = Sets
          .newHashSet(EDGE_4_KNOWS.getId(), EDGE_5_KNOWS.getId(), EDGE_22_KNOWS

            .getId(), EDGE_23_KNOWS.getId());
      } else if (graphData.getId().equals(communityGraphs.getId())) {
        vertexIds = Sets
          .newHashSet(VERTEX_PERSON_ALICE.getId(), VERTEX_PERSON_BOB.getId(),
            VERTEX_PERSON_CAROL

              .getId(), VERTEX_PERSON_DAVE

              .getId());
        edgeIds = Sets
          .newHashSet(EDGE_0_KNOWS.getId(), EDGE_1_KNOWS.getId(), EDGE_2_KNOWS

              .getId(), EDGE_3_KNOWS.getId(), EDGE_4_KNOWS.getId(),
            EDGE_5_KNOWS.getId());
      } else if (graphData.getId().equals(forumGraph.getId())) {
        vertexIds = Sets
          .newHashSet(VERTEX_PERSON_CAROL.getId(), VERTEX_PERSON_DAVE.getId(),
            VERTEX_PERSON_FRANK.getId(), VERTEX_FORUM_GPS.getId());
        edgeIds = Sets
          .newHashSet(EDGE_4_KNOWS.getId(), EDGE_16_HAS_MODERATOR.getId(),
            EDGE_19_HAS_MEMBER.getId(), EDGE_20_HAS_MEMBER.getId());
      }
      persistentGraphData
        .add(graphDataFactory.createGraphData(graphData, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  public static Iterable<PersistentGraphData> createPersistentGraphData() {
    List<PersistentGraphData> persistentGraphData =
      Lists.newArrayListWithExpectedSize(2);

    GraphDataFactory<DefaultGraphData> graphDataFactory =
      new DefaultGraphDataFactory();
    // graph 0
    Long graphID = 0L;
    String graphLabel = "A";
    Map<String, Object> graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    graphProperties.put("k2", "v2");
    Set<Long> vertices = Sets.newHashSetWithExpectedSize(2);
    vertices.add(0L);
    vertices.add(1L);
    Set<Long> edges = Sets.newHashSetWithExpectedSize(2);
    edges.add(2L);
    edges.add(3L);

    persistentGraphData.add(new DefaultPersistentGraphData(
      graphDataFactory.createGraphData(graphID, graphLabel, graphProperties),
      vertices, edges));

    // graph 1
    graphID = 1L;
    graphLabel = "A";
    graphProperties = new HashMap<>();
    graphProperties.put("k1", "v1");
    vertices = Sets.newHashSetWithExpectedSize(2);
    vertices.add(1L);
    vertices.add(2L);
    edges = Sets.newLinkedHashSetWithExpectedSize(2);
    edges.add(4L);
    edges.add(5L);

    persistentGraphData.add(new DefaultPersistentGraphData(
      graphDataFactory.createGraphData(graphID, graphLabel, graphProperties),
      vertices, edges));

    return persistentGraphData;
  }

  public static Iterable<PersistentVertexData<DefaultEdgeData>>
  createPersistentVertexData() {
    List<PersistentVertexData<DefaultEdgeData>> persistentVertexData =
      Lists.newArrayListWithExpectedSize(2);

    PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData,
      DefaultPersistentVertexData>
      persistentVertexDataFactory = new DefaultPersistentVertexDataFactory();
    VertexDataFactory<DefaultVertexData> vertexDataFactory =
      new DefaultVertexDataFactory();
    EdgeDataFactory<DefaultEdgeData> edgeDataFactory =
      new DefaultEdgeDataFactory();
    // vertex 0
    Long vertexId = 0L;
    String vertexLabel = "A";
    Map<String, Object> vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    vertexProperties.put("k2", "v2");
    Set<Long> graphs = Sets.newHashSetWithExpectedSize(2);
    graphs.add(0L);
    graphs.add(1L);
    Set<DefaultEdgeData> outgoingEdgeData = Sets.newHashSetWithExpectedSize(2);
    outgoingEdgeData.add(edgeDataFactory.createEdgeData(0L, "a", 0L, 1L));
    outgoingEdgeData.add(edgeDataFactory.createEdgeData(1L, "b", 0L, 2L));
    Set<DefaultEdgeData> incomingEdgeData = Sets.newHashSetWithExpectedSize(2);
    incomingEdgeData.add(edgeDataFactory.createEdgeData(2L, "a", 1L, 0L));
    incomingEdgeData.add(edgeDataFactory.createEdgeData(3L, "c", 2L, 0L));

    persistentVertexData.add(persistentVertexDataFactory.createVertexData(
      vertexDataFactory
        .createVertexData(vertexId, vertexLabel, vertexProperties, graphs),
      outgoingEdgeData, incomingEdgeData));

    // vertex 1
    vertexId = 1L;
    vertexLabel = "B";
    vertexProperties = new HashMap<>();
    vertexProperties.put("k1", "v1");
    graphs = Sets.newHashSetWithExpectedSize(2);
    graphs.add(1L);
    graphs.add(2L);
    outgoingEdgeData = Sets.newHashSetWithExpectedSize(2);
    outgoingEdgeData.add(edgeDataFactory.createEdgeData(2L, 1L, 0L));
    outgoingEdgeData.add(edgeDataFactory.createEdgeData(4L, 1L, 2L));
    incomingEdgeData = Sets.newHashSetWithExpectedSize(2);
    incomingEdgeData.add(edgeDataFactory.createEdgeData(0L, 0L, 1L));
    incomingEdgeData.add(edgeDataFactory.createEdgeData(5L, 2L, 1L));

    persistentVertexData.add(persistentVertexDataFactory.createVertexData(
      vertexDataFactory
        .createVertexData(vertexId, vertexLabel, vertexProperties, graphs),
      outgoingEdgeData, incomingEdgeData));

    return persistentVertexData;
  }

  public static Iterable<PersistentEdgeData<DefaultVertexData>>
  createPersistentEdgeData() {
    List<PersistentEdgeData<DefaultVertexData>> persistentEdgeData =
      Lists.newArrayListWithExpectedSize(2);

    PersistentEdgeDataFactory<DefaultEdgeData, DefaultVertexData,
      DefaultPersistentEdgeData>
      persistentEdgeDataFactory = new DefaultPersistentEdgeDataFactory();
    VertexDataFactory<DefaultVertexData> vertexDataFactory =
      new DefaultVertexDataFactory();
    EdgeDataFactory<DefaultEdgeData> edgeDataFactory =
      new DefaultEdgeDataFactory();
    // edge 0
    Long edgeId = 0L;
    String edgeLabel = "a";
    Map<String, Object> edgeProperties = new HashMap<>();
    edgeProperties.put("k1", "v1");
    edgeProperties.put("k2", "v2");
    Set<Long> graphs = Sets.newHashSetWithExpectedSize(2);
    graphs.add(0L);
    graphs.add(1L);
    DefaultVertexData edgeSourceData =
      vertexDataFactory.createVertexData(0L, "A");
    DefaultVertexData edgeTargetData =
      vertexDataFactory.createVertexData(1L, "B");

    persistentEdgeData.add(persistentEdgeDataFactory.createEdgeData(
      edgeDataFactory.createEdgeData(edgeId, edgeLabel, edgeSourceData.getId(),
        edgeTargetData.getId(), edgeProperties, graphs), edgeSourceData,
      edgeTargetData));

    // edge 1
    edgeId = 1L;
    edgeLabel = "b";
    edgeProperties = Maps.newHashMapWithExpectedSize(1);
    edgeProperties.put("k1", "v1");
    graphs = Sets.newHashSetWithExpectedSize(2);
    graphs.add(1L);
    graphs.add(2L);
    edgeSourceData = vertexDataFactory.createVertexData(0L, "A");
    edgeTargetData = vertexDataFactory.createVertexData(2L, "C");

    persistentEdgeData.add(persistentEdgeDataFactory.createEdgeData(
      edgeDataFactory.createEdgeData(edgeId, edgeLabel, edgeSourceData.getId(),
        edgeTargetData.getId(), edgeProperties, graphs), edgeSourceData,
      edgeTargetData));

    return persistentEdgeData;
  }

  /**
   * Checks data consistency with {@code createPersistentGraphData()}.
   *
   * @param graphStore graph store
   */
  public static void validateGraphData(
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore) {
    // g0
    GraphData g = graphStore.readGraphData(0L);
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
    g = graphStore.readGraphData(1L);
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
  public static void validateVertexData(
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore) {
    // vertex 0
    VertexData v = graphStore.readVertexData(0L);
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
    assertTrue(v.getGraphs().contains(0L));
    assertTrue(v.getGraphs().contains(1L));

    // vertex 1
    v = graphStore.readVertexData(1L);
    assertNotNull(v);
    assertEquals("B", v.getLabel());
    propertyKeys = Lists.newArrayList(v.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", v.getProperty("k1"));
    assertEquals(2, v.getGraphCount());
    assertTrue(v.getGraphs().contains(1L));
    assertTrue(v.getGraphs().contains(2L));
  }

  /**
   * Checks data consistency with {@code createPersistentEdgeData()}.
   *
   * @param graphStore graph store
   */
  public static void validateEdgeData(
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore) {
    // edge 0
    EdgeData e = graphStore.readEdgeData(0L);
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
    assertTrue(e.getGraphs().contains(0L));
    assertTrue(e.getGraphs().contains(1L));

    // edge 1
    e = graphStore.readEdgeData(1L);
    assertNotNull(e);
    assertEquals("b", e.getLabel());
    assertEquals(new Long(0L), e.getSourceVertexId());
    assertEquals(new Long(2L), e.getTargetVertexId());
    assertEquals(1L, e.getPropertyCount());
    propertyKeys = Lists.newArrayList(e.getPropertyKeys());
    assertEquals(1, propertyKeys.size());
    assertEquals("v1", e.getProperty("k1"));
    assertEquals(2, e.getGraphCount());
    assertTrue(e.getGraphs().contains(1L));
    assertTrue(e.getGraphs().contains(2L));

  }
}
