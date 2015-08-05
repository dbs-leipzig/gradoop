package org.gradoop.model.impl;

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.impl.operators.EPGLabelPropagation;
import org.gradoop.model.impl.operators.EPGLabelPropagationAlgorithm;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGLabelPropagationTest extends EPFlinkTest {
  private EPGraphStore graphStore;
  final String propertyKey = EPGLabelPropagationAlgorithm.CURRENT_VALUE;

  public EPGLabelPropagationTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testLabelPropagationWithCallByPropertyKey() throws Exception {
    EPGraph inputGraph = graphStore.getGraph(2L);
    EPGraphCollection labeledGraph = inputGraph
      .callForCollection(new EPGLabelPropagation(2, propertyKey, env));
    labeledGraph.getGellyGraph().getVertices().print();
    assertNotNull("graph collection is null", inputGraph);
    assertEquals("wrong number of graphs", 3l, labeledGraph.size());
    assertEquals("wrong number of vertices", 4l,
      labeledGraph.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 0l,
      labeledGraph.getGraph().getEdgeCount());
  }
}
