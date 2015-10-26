package org.gradoop.model.impl.algorithms.btg;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BTGGradoopTest extends FlinkTestBase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public BTGGradoopTest(MultipleProgramsTestBase.TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile = BTGGradoopTest.class.getResource("/btg/btg_nodes").getFile();
    String edgeFile = BTGGradoopTest.class.getResource("/btg/btg_edges").getFile();
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, env);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      databaseGraph = graphStore.getDatabaseGraph();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      btgGraph = databaseGraph.callForCollection(
      new BTG<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(50, env));
    assertNotNull("graph collection is null", databaseGraph);
    assertEquals("wrong number of graphs", 2l, btgGraph.getGraphCount());
    assertEquals("wrong number of vertices", 9l,
      btgGraph.getGraph(4L).getVertexCount());
    assertEquals("wrong number of vertices", 11l,
      btgGraph.getGraph(9L).getVertexCount());
    assertEquals("wrong number of edges", 0l,
      btgGraph.getGraph(9L).getEdgeCount());
    assertEquals("wrong number of edges", 0l,
      btgGraph.getGraph(4L).getEdgeCount());
    validateConnectedIIGResult(BTGAlgorithmTestHelper
      .parseResultDefaultVertexData(btgGraph.toGellyGraph().getVertices()
        .collect()));
  }

  private void validateConnectedIIGResult(Map<Long, List<Long>> btgIDs) {
    assertEquals(16, btgIDs.size());
    // master data nodes BTG 1 and 2
    assertEquals(2, btgIDs.get(0L).size());
    assertTrue(btgIDs.get(0L).contains(4L));
    assertTrue(btgIDs.get(0L).contains(9L));
    assertEquals(2, btgIDs.get(1L).size());
    assertTrue(btgIDs.get(1L).contains(4L));
    assertTrue(btgIDs.get(1L).contains(9L));
    assertEquals(2, btgIDs.get(2L).size());
    assertTrue(btgIDs.get(2L).contains(4L));
    assertTrue(btgIDs.get(2L).contains(9L));
    assertEquals(2, btgIDs.get(3L).size());
    assertTrue(btgIDs.get(3L).contains(4L));
    assertTrue(btgIDs.get(3L).contains(9L));
    // transactional data nodes BTG 1
    assertEquals(1, btgIDs.get(4L).size());
    assertTrue(btgIDs.get(4L).contains(4L));
    assertEquals(1, btgIDs.get(5L).size());
    assertTrue(btgIDs.get(5L).contains(4L));
    assertEquals(1, btgIDs.get(6L).size());
    assertTrue(btgIDs.get(6L).contains(4L));
    assertEquals(1, btgIDs.get(7L).size());
    assertTrue(btgIDs.get(7L).contains(4L));
    assertEquals(1, btgIDs.get(8L).size());
    assertTrue(btgIDs.get(8L).contains(4L));
    assertEquals(1, btgIDs.get(9L).size());
    // transactional data nodes BTG 2
    assertTrue(btgIDs.get(9L).contains(9L));
    assertEquals(1, btgIDs.get(10L).size());
    assertTrue(btgIDs.get(10L).contains(9L));
    assertEquals(1, btgIDs.get(11L).size());
    assertTrue(btgIDs.get(11L).contains(9L));
    assertEquals(1, btgIDs.get(12L).size());
    assertTrue(btgIDs.get(12L).contains(9L));
    assertEquals(1, btgIDs.get(13L).size());
    assertTrue(btgIDs.get(13L).contains(9L));
    assertEquals(1, btgIDs.get(14L).size());
    assertTrue(btgIDs.get(14L).contains(9L));
    assertEquals(1, btgIDs.get(15L).size());
    assertTrue(btgIDs.get(15L).contains(9L));
  }
}
