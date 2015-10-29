package org.gradoop.model.impl.operators.auxiliary;

import junit.framework.TestCase;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
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
    UnaryFunction<DefaultVertexData, List<Long>> function =
      new SplitByModulo();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new OverlapSplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        function));
    labeledGraphCollection.getGraphHeads();
    assertNotNull("graph collection is null", labeledGraphCollection);
    TestCase.assertEquals("wrong number of graphs", 3l,
      labeledGraphCollection.getGraphCount());
    TestCase.assertEquals("wrong number of vertices", 11l,
      labeledGraphCollection.getVertexCount());
    TestCase.assertEquals("wrong number of edges", 8l,
      labeledGraphCollection.getEdgeCount());
  }

  private static class SplitByModulo implements
    UnaryFunction<DefaultVertexData, List<Long>> {
    @Override
    public List<Long> execute(DefaultVertexData vertex) throws
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
