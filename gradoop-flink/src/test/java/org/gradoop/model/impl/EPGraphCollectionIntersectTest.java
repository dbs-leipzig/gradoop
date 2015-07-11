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
public class EPGraphCollectionIntersectTest extends EPFlinkTest {

  private EPGraphStore graphStore;

  public EPGraphCollectionIntersectTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0 1 2, 0 1, 2, 6, 10", "0, 1, 0, 0, 0"})
  public void testIntersect(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();
    EPGraphCollection collection1 =
      graphColl.getGraphs(extractGraphIDs(firstColl));
    EPGraphCollection collection2 =
      graphColl.getGraphs(extractGraphIDs(secondColl));

    EPGraphCollection intersectColl =
      collection1.alternateIntersect(collection2);

    assertNotNull("graph collection is null", intersectColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      intersectColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      intersectColl.getGraph().getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      intersectColl.getGraph().getEdgeCount());
  }
}
