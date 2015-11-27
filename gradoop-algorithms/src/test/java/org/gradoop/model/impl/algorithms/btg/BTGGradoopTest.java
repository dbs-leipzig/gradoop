package org.gradoop.model.impl.algorithms.btg;//package org.gradoop.model.impl.algorithms.data.json.btg;
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.test.util.MultipleProgramsTestBase;
//import org.gradoop.model.FlinkTestBase;
//import org.gradoop.model.impl.LogicalGraph;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.pojo.EdgePojo;
//import org.gradoop.model.impl.pojo.GraphHeadPojo;
//import org.gradoop.model.impl.pojo.VertexPojo;
//import org.gradoop.model.impl.EPGMDatabase;
//import org.gradoop.model.impl.GraphCollection;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.Assert.*;
//
//@RunWith(Parameterized.class)
//public class BTGGradoopTest extends FlinkTestBase {
//  @Rule
//  public TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//  public BTGGradoopTest(MultipleProgramsTestBase.TestExecutionMode mode) {
//    super(mode);
//  }
//
//  @Test
//  public void testFromJsonFile() throws Exception {
//    String vertexFile = BTGGradoopTest.class.getResource("/data.json.btg/btg_nodes").getFile();
//    String edgeFile = BTGGradoopTest.class.getResource("/data.json.btg/btg_edges").getFile();
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    EPGMDatabase<VertexPojo, EdgePojo, GraphHeadPojo>
//      graphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, env);
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      databaseGraph = graphStore.getDatabaseGraph();
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
//      btgGraph = databaseGraph.callForCollection(
//      new BTG<VertexPojo, EdgePojo, GraphHeadPojo>(50));
//    assertNotNull("graph collection is null", databaseGraph);
//    assertEquals("wrong number of graphs", 2l, btgGraph.getGraphCount());
//    assertEquals("wrong number of vertices", 9l,
//      btgGraph.getGraph(GradoopId.fromLong(4L)).getVertexCount());
//    assertEquals("wrong number of vertices", 11l,
//      btgGraph.getGraph(GradoopId.fromLong(9L)).getVertexCount());
//    assertEquals("wrong number of edges", 0l,
//      btgGraph.getGraph(GradoopId.fromLong(9L)).getEdgeCount());
//    assertEquals("wrong number of edges", 0l,
//      btgGraph.getGraph(GradoopId.fromLong(4L)).getEdgeCount());
//    validateConnectedIIGResult(BTGAlgorithmTestHelper
//      .parseResultVertexPojos(btgGraph.toGellyGraph().getVertices().collect()));
//  }
//
//  private void validateConnectedIIGResult(Map<GradoopId, List<GradoopId>> btgIDs) {
//    assertEquals(16, btgIDs.size());
//    // master data nodes BTG 1 and 2
//    assertEquals(2, btgIDs.get(GradoopId.fromLong(0L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(0L)).contains(4L));
//    assertTrue(btgIDs.get(GradoopId.fromLong(0L)).contains(9L));
//    assertEquals(2, btgIDs.get(GradoopId.fromLong(1L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(1L)).contains(4L));
//    assertTrue(btgIDs.get(GradoopId.fromLong(1L)).contains(9L));
//    assertEquals(2, btgIDs.get(GradoopId.fromLong(2L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(2L)).contains(4L));
//    assertTrue(btgIDs.get(GradoopId.fromLong(2L)).contains(9L));
//    assertEquals(2, btgIDs.get(GradoopId.fromLong(3L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(3L)).contains(4L));
//    assertTrue(btgIDs.get(GradoopId.fromLong(3L)).contains(9L));
//    // transactional data nodes BTG 1
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(4L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(4L)).contains(4L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(5L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(5L)).contains(4L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(6L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(6L)).contains(4L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(7L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(7L)).contains(4L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(8L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(8L)).contains(4L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(9L)).size());
//    // transactional data nodes BTG 2
//    assertTrue(btgIDs.get(GradoopId.fromLong(9L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(10L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(10L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(11L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(11L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(12L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(12L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(13L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(13L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(14L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(14L)).contains(9L));
//    assertEquals(1, btgIDs.get(GradoopId.fromLong(15L)).size());
//    assertTrue(btgIDs.get(GradoopId.fromLong(15L)).contains(9L));
//  }
//}
