package org.gradoop.model.impl;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.SplitBy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class LogicalGraphSplitByTest extends FlinkTestBase {
  public LogicalGraphSplitByTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSplitBy() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L);
    UnaryFunction<Vertex<Long, DefaultVertexData>, Long> function =
      new SplitByIdOddOrEven();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new SplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        function, ExecutionEnvironment.getExecutionEnvironment()));
    labeledGraphCollection.getGellyGraph().getVertices().print();
    labeledGraphCollection.getGellyGraph().getEdges().print();
    assertNotNull("graph collection is null", labeledGraphCollection);
    assertEquals("wrong number of graphs", 2l, labeledGraphCollection.size());
    assertEquals("wrong number of vertices", 3l,
      labeledGraphCollection.getTotalVertexCount());
    assertEquals("wrong number of edges", 1l,
      labeledGraphCollection.getTotalEdgeCount());
  }

  private static class SplitByIdOddOrEven implements
    UnaryFunction<Vertex<Long, DefaultVertexData>, Long> {
    @Override
    public Long execute(Vertex<Long, DefaultVertexData> entity) throws
      Exception {
      return (entity.getId() % 2) - 2;
    }
  }
}
