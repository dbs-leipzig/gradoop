package org.gradoop.model.impl;

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.impl.operators.LabelPropagation;
import org.gradoop.model.impl.operators.LabelPropagationAlgorithm;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphLabelPropagationTest extends EPFlinkTest {
  private EPGraphStore graphStore;
  final String propertyKey = LabelPropagationAlgorithm.PROPERTYKEY;

  public EPGraphLabelPropagationTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testLabelPropagationWithCallByPropertyKey() throws Exception {
    EPGraph inputGraph = graphStore.getGraph(2L);
    EPGraphCollection labeledGraph =
      inputGraph.callForCollection(new LabelPropagation(2, propertyKey, env));
    assertNotNull("graph collection is null", inputGraph);
    assertEquals("wrong number of graphs", 3l, labeledGraph.size());
    assertEquals("wrong number of vertices", 4l,
      labeledGraph.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 2l,
      labeledGraph.getGraph().getEdgeCount());
  }
}
