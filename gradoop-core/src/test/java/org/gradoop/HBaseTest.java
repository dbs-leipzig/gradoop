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
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultEdgeDataFactory;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultGraphDataFactory;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.DefaultVertexDataFactory;
import org.gradoop.storage.*;
import org.gradoop.storage.hbase.*;
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

import static org.junit.Assert.*;

/**
 * Used for test cases that need a HDFS/HBase/MR mini cluster to run.
 * Initializes a test cluster before the first test runs.
 */
public abstract class HBaseTest extends GradoopTest {

  protected static HBaseTestingUtility utility;

  protected EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
  createEmptyEPGMStore() {
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
  protected EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
  openEPGMStore() {
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
   * Open EPGMStore for test purposes.
   *
   * @param tablePrefix table prefix for custom (parallel) tables
   * @return EPGMStore with vertices and edges
   */
  protected EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
  openEPGMStore(
    String tablePrefix) {
    Configuration config = utility.getConfiguration();
    VertexDataHandler<DefaultVertexData, DefaultEdgeData> vertexDataHandler =
      new DefaultVertexDataHandler<>(new DefaultVertexDataFactory());
    EdgeDataHandler<DefaultEdgeData, DefaultVertexData> edgeDataHandler =
      new DefaultEdgeDataHandler<>(new DefaultEdgeDataFactory());
    GraphDataHandler<DefaultGraphData> graphDataHandler =
      new DefaultGraphDataHandler<>(new DefaultGraphDataFactory());
    return HBaseEPGMStoreFactory
      .createOrOpenEPGMStore(config, vertexDataHandler, edgeDataHandler,
        graphDataHandler, tablePrefix + GConstants.DEFAULT_TABLE_VERTICES,
        tablePrefix + GConstants.DEFAULT_TABLE_EDGES,
        tablePrefix + GConstants.DEFAULT_TABLE_GRAPHS);
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
   * Starts the mini cluster once for all test cases implementing this class.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility(HBaseConfiguration.create());
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }
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

  protected Collection<PersistentVertexData<DefaultEdgeData>>
  createPersistentSocialVertexData() {
    Collection<DefaultVertexData> vertexDataCollection =
      createVertexDataCollection();
    List<PersistentVertexData<DefaultEdgeData>> persistentVertexData =
      Lists.newArrayListWithExpectedSize(vertexDataCollection.size());
    PersistentVertexDataFactory<DefaultVertexData, DefaultEdgeData,
      DefaultPersistentVertexData>
      vertexDataFactory = new DefaultPersistentVertexDataFactory();


    Set<DefaultEdgeData> outEdges = null;
    Set<DefaultEdgeData> inEdges = null;
    for (DefaultVertexData vertexData : vertexDataCollection) {
      if (vertexData.getId().equals(alice.getId())) {
        outEdges = Sets.newHashSet(edge0, edge8);
        inEdges = Sets.newHashSet(edge1, edge6, edge15, edge17);

      } else if (vertexData.getId().equals(bob.getId())) {
        outEdges = Sets.newHashSet(edge1, edge2);
        inEdges = Sets.newHashSet(edge0, edge3, edge18, edge21);
      } else if (vertexData.getId().equals(carol.getId())) {
        outEdges = Sets.newHashSet(edge3, edge4);
        inEdges = Sets.newHashSet(edge2, edge5, edge19, edge22);
      } else if (vertexData.getId().equals(dave.getId())) {
        outEdges = Sets.newHashSet(edge5, edge9);
        inEdges = Sets.newHashSet(edge4, edge16, edge20, edge23);
      } else if (vertexData.getId().equals(eve.getId())) {
        outEdges = Sets.newHashSet(edge6, edge7, edge21);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(frank.getId())) {
        outEdges = Sets.newHashSet(edge10, edge22, edge23);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(tagDatabases.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(edge7, edge8, edge11);
      } else if (vertexData.getId().equals(tagGraphs.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(edge12, edge13);
      } else if (vertexData.getId().equals(tagHadoop.getId())) {
        outEdges = Sets.newHashSet();
        inEdges = Sets.newHashSet(edge9, edge10, edge14);
      } else if (vertexData.getId().equals(forumGDBS.getId())) {
        outEdges = Sets.newHashSet(edge11, edge12, edge15, edge17, edge18);
        inEdges = Sets.newHashSet();
      } else if (vertexData.getId().equals(forumGPS.getId())) {
        outEdges = Sets.newHashSet(edge13, edge14, edge16, edge19, edge20);
        inEdges = Sets.newHashSet();
      }
      persistentVertexData
        .add(vertexDataFactory.createVertexData(vertexData, outEdges, inEdges));
    }
    return persistentVertexData;
  }

  protected Collection<PersistentEdgeData<DefaultVertexData>>
  createPersistentSocialEdgeData() {
    Collection<DefaultEdgeData> edgeDataCollection = createEdgeDataCollection();
    List<PersistentEdgeData<DefaultVertexData>> persistentEdgeData =
      Lists.newArrayListWithExpectedSize(edgeDataCollection.size());
    PersistentEdgeDataFactory<DefaultEdgeData, DefaultVertexData,
      DefaultPersistentEdgeData>
      edgeDataFactory = new DefaultPersistentEdgeDataFactory();

    DefaultVertexData sourceVertexData = null;
    DefaultVertexData targetVertexData = null;
    for (DefaultEdgeData edgeData : edgeDataCollection) {
      if (edgeData.getId().equals(edge0.getId())) {
        sourceVertexData = alice;
        targetVertexData = bob;
      } else if (edgeData.getId().equals(edge1.getId())) {
        sourceVertexData = bob;
        targetVertexData = alice;
      } else if (edgeData.getId().equals(edge2.getId())) {
        sourceVertexData = bob;
        targetVertexData = carol;
      } else if (edgeData.getId().equals(edge3.getId())) {
        sourceVertexData = carol;
        targetVertexData = bob;
      } else if (edgeData.getId().equals(edge4.getId())) {
        sourceVertexData = carol;
        targetVertexData = dave;
      } else if (edgeData.getId().equals(edge5.getId())) {
        sourceVertexData = dave;
        targetVertexData = carol;
      } else if (edgeData.getId().equals(edge6.getId())) {
        sourceVertexData = eve;
        targetVertexData = alice;
      } else if (edgeData.getId().equals(edge7.getId())) {
        sourceVertexData = eve;
        targetVertexData = tagDatabases;
      } else if (edgeData.getId().equals(edge8.getId())) {
        sourceVertexData = alice;
        targetVertexData = tagDatabases;
      } else if (edgeData.getId().equals(edge9.getId())) {
        sourceVertexData = dave;
        targetVertexData = tagHadoop;
      } else if (edgeData.getId().equals(edge10.getId())) {
        sourceVertexData = frank;
        targetVertexData = tagHadoop;
      } else if (edgeData.getId().equals(edge11.getId())) {
        sourceVertexData = forumGDBS;
        targetVertexData = tagDatabases;
      } else if (edgeData.getId().equals(edge12.getId())) {
        sourceVertexData = forumGDBS;
        targetVertexData = tagGraphs;
      } else if (edgeData.getId().equals(edge13.getId())) {
        sourceVertexData = forumGPS;
        targetVertexData = tagGraphs;
      } else if (edgeData.getId().equals(edge14.getId())) {
        sourceVertexData = forumGPS;
        targetVertexData = tagHadoop;
      } else if (edgeData.getId().equals(edge15.getId())) {
        sourceVertexData = forumGDBS;
        targetVertexData = alice;
      } else if (edgeData.getId().equals(edge16.getId())) {
        sourceVertexData = forumGPS;
        targetVertexData = dave;
      } else if (edgeData.getId().equals(edge17.getId())) {
        sourceVertexData = forumGDBS;
        targetVertexData = alice;
      } else if (edgeData.getId().equals(edge18.getId())) {
        sourceVertexData = forumGDBS;
        targetVertexData = bob;
      } else if (edgeData.getId().equals(edge19.getId())) {
        sourceVertexData = forumGPS;
        targetVertexData = carol;
      } else if (edgeData.getId().equals(edge20.getId())) {
        sourceVertexData = forumGPS;
        targetVertexData = dave;
      } else if (edgeData.getId().equals(edge21.getId())) {
        sourceVertexData = eve;
        targetVertexData = bob;
      } else if (edgeData.getId().equals(edge22.getId())) {
        sourceVertexData = frank;
        targetVertexData = carol;
      } else if (edgeData.getId().equals(edge23.getId())) {
        sourceVertexData = frank;
        targetVertexData = dave;
      }
      persistentEdgeData.add(edgeDataFactory
        .createEdgeData(edgeData, sourceVertexData, targetVertexData));
    }

    return persistentEdgeData;
  }

  protected Collection<PersistentGraphData> createPersistentSocialGraphData() {
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
        vertexIds = Sets.newHashSet(alice.getId(), bob.getId(), eve.getId());
        edgeIds = Sets.newHashSet(edge0.getId(), edge1.getId(), edge6.getId(),
          edge21.getId());
      } else if (graphData.getId().equals(communityHadoop.getId())) {
        vertexIds = Sets.newHashSet(carol.getId(), dave.getId(), frank.getId());
        edgeIds = Sets.newHashSet(edge4.getId(), edge5.getId(), edge22.getId(),
          edge23.getId());
      } else if (graphData.getId().equals(communityGraphs.getId())) {
        vertexIds = Sets
          .newHashSet(alice.getId(), bob.getId(), carol.getId(), dave.getId());
        edgeIds = Sets.newHashSet(edge0.getId(), edge1.getId(), edge2.getId(),
          edge3.getId(), edge4.getId(), edge5.getId());
      } else if (graphData.getId().equals(forumGraph.getId())) {
        vertexIds = Sets.newHashSet(carol.getId(), dave.getId(), frank.getId(),
          forumGPS.getId());
        edgeIds = Sets.newHashSet(edge4.getId(), edge16.getId(), edge19.getId(),
          edge20.getId());
      }
      persistentGraphData
        .add(graphDataFactory.createGraphData(graphData, vertexIds, edgeIds));
    }

    return persistentGraphData;
  }

  protected Iterable<PersistentGraphData> createPersistentGraphData() {
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

  protected Iterable<PersistentVertexData<DefaultEdgeData>>
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

  protected Iterable<PersistentEdgeData<DefaultVertexData>>
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
  protected void validateGraphData(
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
  protected void validateVertexData(
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
  protected void validateEdgeData(
    EPGMStore<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore) {
    // edge 0
    EdgeData e = graphStore.readEdgeData(0L);
    System.out.println("*** " + e);
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
