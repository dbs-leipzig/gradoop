package org.gradoop.model.impl.btg;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.btg.BTG;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class BTGTest extends FlinkTestBase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public BTGTest(MultipleProgramsTestBase.TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile = BTGTest.class.getResource("/btg/btg_nodes").getFile();
    String edgeFile = BTGTest.class.getResource("/btg/btg_edges").getFile();
    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile,
      ExecutionEnvironment.getExecutionEnvironment());
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      databaseGraph = graphStore.getDatabaseGraph();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      btgGraph = databaseGraph.callForCollection(
      new BTG<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(50,
        ExecutionEnvironment.getExecutionEnvironment()));
    assertNotNull("graph collection is null", databaseGraph);
    assertEquals("wrong number of graphs", 2l, btgGraph.size());
    assertEquals("wrong number of vertices", 9l,
      btgGraph.getGraph(4L).getVertexCount());
    assertEquals("wrong number of vertices", 11l,
      btgGraph.getGraph(9L).getVertexCount());
    assertEquals("wrong number of edges", 0l,
      btgGraph.getGraph(9L).getEdgeCount());
    assertEquals("wrong number of edges", 0l,
      btgGraph.getGraph(4L).getEdgeCount());
  }
}
