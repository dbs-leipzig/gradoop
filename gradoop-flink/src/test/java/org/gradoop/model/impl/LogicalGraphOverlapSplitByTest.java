package org.gradoop.model.impl;

import junit.framework.TestCase;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.OverlapSplitBy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class LogicalGraphOverlapSplitByTest extends FlinkTestBase {
  public LogicalGraphOverlapSplitByTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingResultGraphs() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = getGraphStore().getDatabaseGraph();
    UnaryFunction<Vertex<Long, DefaultVertexData>, List<Long>> function =
      new SplitByModulo();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new OverlapSplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        function, ExecutionEnvironment.getExecutionEnvironment()));
    labeledGraphCollection.getSubgraphs();
    assertNotNull("graph collection is null", labeledGraphCollection);
    TestCase.assertEquals("wrong number of graphs", 3l,
      labeledGraphCollection.size());
    TestCase.assertEquals("wrong number of vertices", 11l,
      labeledGraphCollection.getTotalVertexCount());
    TestCase.assertEquals("wrong number of edges", 8l,
      labeledGraphCollection.getTotalEdgeCount());
  }

  private static class SplitByModulo implements
    UnaryFunction<Vertex<Long, DefaultVertexData>, List<Long>> {
    @Override
    public List<Long> execute(Vertex<Long, DefaultVertexData> vertex) throws
      Exception {
      List<Long> list = new ArrayList<>();
      boolean inNewGraph = false;
      if (vertex.getId() % 2 == 0) {
        list.add(-2l);
        inNewGraph = true;
      }
      if (vertex.getId() % 3 == 0) {
        list.add(-3l);
        inNewGraph = true;
      }
      if (!inNewGraph) {
        list.add(-1l);
      }
      return list;
    }
  }
}
