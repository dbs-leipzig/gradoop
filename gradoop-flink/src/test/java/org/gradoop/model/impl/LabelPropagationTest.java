package org.gradoop.model.impl;

import org.gradoop.model.FlinkTest;
import org.gradoop.model.impl.operators.labelpropagation
  .EPGLabelPropagationAlgorithm;
import org.gradoop.model.impl.operators.labelpropagation.LabelPropagation;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LabelPropagationTest extends FlinkTest {
  private EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
    graphStore;
  final String propertyKey = EPGLabelPropagationAlgorithm.CURRENT_VALUE;

  public LabelPropagationTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testLabelPropagationWithCallByPropertyKey() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(2L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new LabelPropagation<DefaultVertexData, DefaultEdgeData,
        DefaultGraphData>(
        2, propertyKey, env));
    labeledGraphCollection.getGellyGraph().getVertices().print();
    assertNotNull("graph collection is null", labeledGraphCollection);
    assertEquals("wrong number of graphs", 3l, labeledGraphCollection.size());
    assertEquals("wrong number of vertices", 4l,
      labeledGraphCollection.getTotalVertexCount());
    assertEquals("wrong number of edges", 0l,
      labeledGraphCollection.getTotalEdgeCount());
  }
}
