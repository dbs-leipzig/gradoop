package org.gradoop.model.impl;

import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTest;
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.operators.SplitBy;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphSplitByTest extends FlinkTest {
  private EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
    graphStore;

  public EPGraphSplitByTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testSplitBy() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L);
    LongFromVertexFunction<DefaultVertexData> function =
      new SplitByIdOddOrEven();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new SplitBy<DefaultVertexData, DefaultEdgeData, DefaultGraphData>(
        function, env));
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
    LongFromVertexFunction<DefaultVertexData> {
    @Override
    public Long extractLong(Vertex<Long, DefaultVertexData> vertex) {
      return (vertex.getId() % 2) - 2;
    }
  }
}
