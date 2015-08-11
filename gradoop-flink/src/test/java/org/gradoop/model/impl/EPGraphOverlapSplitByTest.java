package org.gradoop.model.impl;

import org.apache.flink.graph.Vertex;
import org.gradoop.model.OverlapSplitByGraphTest;
import org.gradoop.model.helper.LongSetFromVertexFunction;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphOverlapSplitByTest extends OverlapSplitByGraphTest {
  private EPGraphStore graphStore;

  public EPGraphOverlapSplitByTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testOverlapSplitBy() throws Exception {
    EPGraph inputGraph = graphStore.getGraph(0L);
    LongSetFromVertexFunction function = new SplitByPropertyString();
    EPGraphCollection labeledGraph =
      inputGraph.callForCollection(new OverlapSplitBy(function, env));
    labeledGraph.getGellyGraph().getVertices().print();
    labeledGraph.getGellyGraph().getEdges().print();
    assertNotNull("graph collection is null", labeledGraph);
    assertEquals("wrong number of graphs", 4l, labeledGraph.size());
    assertEquals("wrong number of vertices", 3l,
      labeledGraph.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 4l,
      labeledGraph.getGraph().getEdgeCount());
  }

  private static class SplitByPropertyString implements
    LongSetFromVertexFunction {
    @Override
    public Set<Long> extractLongSet(Vertex<Long, EPFlinkVertexData> vertex) {
      String btgid =
        (String) vertex.getValue().getProperty(PROPERTY_KEY_VERTEX_BTGID);
      Set<Long> btgSet = new HashSet<>();
      String[] ids = btgid.split(",");
      for (int i = 0; i < ids.length; i++) {
        btgSet.add(Long.parseLong(ids[i]));
      }
      return btgSet;
    }
  }
}
