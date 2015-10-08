package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphSplitByTest extends FlinkTestBase {
  public LogicalGraphSplitByTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSplitBy() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = getGraphStore().getGraph(0L);
    UnaryFunction<DefaultVertexData, Long> function = new SplitByIdOddOrEven();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new SplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        new SplitByIdOddOrEven(), getExecutionEnvironment()));

    List<DefaultVertexData> oldVertices = Lists.newArrayList();
    inputGraph.getVertexData().output(
      new LocalCollectionOutputFormat<>(oldVertices));

    List<DefaultVertexData> newVertices = Lists.newArrayList();
    labeledGraphCollection.getVertexData().output(
      new LocalCollectionOutputFormat<>(newVertices));

    List<DefaultEdgeData> newEdges = Lists.newArrayList();
    labeledGraphCollection.getEdgeData().output(
      new LocalCollectionOutputFormat<>(newEdges));

      assertNotNull("graph collection is null", labeledGraphCollection);
    assertEquals("wrong number of graphs", 2L,
      labeledGraphCollection.getGraphCount());
    assertEquals("wrong number of vertices", 3L, newVertices.size());
    assertEquals("wrong number of edges", 1L, newEdges.size());

    for (int i = 0; i < newVertices.size(); i++) {
      DefaultVertexData oldVertex = oldVertices.get(i);
      DefaultVertexData newVertex = newVertices.get(i);
      assertTrue((oldVertex.getGraphCount() + 1) == newVertex.getGraphCount());
      assertTrue(newVertex.getGraphs().containsAll(oldVertex.getGraphs()));
      assertTrue(newVertex.getGraphs().contains(function.execute(newVertex)));
    }
  }

  private static class SplitByIdOddOrEven implements
    UnaryFunction<DefaultVertexData, Long> {
    @Override
    public Long execute(DefaultVertexData entity) throws
      Exception {
      return (entity.getId() % 2) - 2;
    }
  }
}
