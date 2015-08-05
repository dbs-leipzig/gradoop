package org.gradoop.model.impl;

import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.helper.LongFromVertexFunction;
import org.gradoop.model.impl.operators.SplitBy;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphSplitByTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphSplitByTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testSplitBy() throws Exception {
    EPGraph inputGraph = graphStore.getGraph(0L);
    LongFromVertexFunction function = new SplitByIdOddOrEven();
    EPGraphCollection labeledGraph =
      inputGraph.callForCollection(new SplitBy(function, env));
    labeledGraph.getGellyGraph().getVertices().print();
    labeledGraph.getGellyGraph().getEdges().print();
    assertNotNull("graph collection is null", labeledGraph);
    assertEquals("wrong number of graphs", 2l, labeledGraph.size());
    assertEquals("wrong number of vertices", 3l,
      labeledGraph.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 1l,
      labeledGraph.getGraph().getEdgeCount());
  }

  private static class SplitByIdOddOrEven implements LongFromVertexFunction {
    @Override
    public Long extractLong(Vertex<Long, EPFlinkVertexData> vertex) {
      return (vertex.getId() % 2) - 2;
    }
  }
}
