package org.gradoop.model.impl;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.SplitBy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LogicalGraphSplitByTest extends FlinkTestBase {
  public LogicalGraphSplitByTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSplitBy() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = getGraphStore().getGraph(0L);
    UnaryFunction<Vertex<Long, DefaultVertexData>, Long> function =
      new SplitByIdOddOrEven();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new SplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        function, ExecutionEnvironment.getExecutionEnvironment()));
    assertNotNull("graph collection is null", labeledGraphCollection);
    assertEquals("wrong number of graphs", 2L, labeledGraphCollection.size());
    assertEquals("wrong number of vertices", 3L,
      labeledGraphCollection.getVertexCount());
    List<Vertex<Long, DefaultVertexData>> oldVertices =
      inputGraph.getGellyGraph().getVertices().collect();
    List<Vertex<Long, DefaultVertexData>> newVertices =
      labeledGraphCollection.getGellyGraph().getVertices().collect();
    for (int i = 0; i < newVertices.size(); i++) {
      Vertex<Long, DefaultVertexData> oldVertex = oldVertices.get(i);
      Vertex<Long, DefaultVertexData> newVertex = newVertices.get(i);
      assertTrue((oldVertex.getValue().getGraphCount() + 1) ==
        newVertex.getValue().getGraphCount());
      assertTrue(newVertex.getValue().getGraphs()
        .containsAll(oldVertex.getValue().getGraphs()));
      assertTrue(
        newVertex.getValue().getGraphs().contains(function.execute(newVertex)));
    }
    assertEquals("wrong number of edges", 1L,
      labeledGraphCollection.getEdgeCount());
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
