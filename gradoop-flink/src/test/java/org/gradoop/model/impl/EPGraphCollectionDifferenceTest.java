package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class EPGraphCollectionDifferenceTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphCollectionDifferenceTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0 1 2, 0, 2, 5, 8", "0 1, 2 3, 2, 6, 8"})
  public void testDifference(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();
    EPGraphCollection collection1 =
      graphColl.getGraphs(extractGraphIDs(firstColl));
    EPGraphCollection collection2 =
      graphColl.getGraphs(extractGraphIDs(secondColl));

    EPGraphCollection differenceColl =
      collection1.difference(collection2);

    assertNotNull("graph collection is null", differenceColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      differenceColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      differenceColl.getGraph().getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      differenceColl.getGraph().getEdgeCount());
  }
}
