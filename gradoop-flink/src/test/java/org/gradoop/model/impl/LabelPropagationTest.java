package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.impl.operators.LabelPropagation;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by niklas on 16.07.15.
 */

@RunWith(JUnitParamsRunner.class)
public class LabelPropagationTest extends EPFlinkTest {


  private EPGraphStore graphStore;

  public LabelPropagationTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testLabelPropagation() throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraph graph = graphColl.getGraph(2l);

    LabelPropagation lp = new LabelPropagation(2, "value", env);

    EPGraphCollection resultColl = graph.callForCollection(lp);

/*
    resultColl.getGraph().getVertices().print();
    System.out.println("asdf");
    resultColl.getGraph().getEdges().print();
    System.out.println("fdsa");
*/
    assertNotNull("graph collection was null", resultColl);
    assertEquals("wrong number of graphs", 3, resultColl.size());
    assertEquals("wrong number of vertices", 4,
      resultColl.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 2,
      resultColl.getGraph().getEdgeCount());


  }
}
