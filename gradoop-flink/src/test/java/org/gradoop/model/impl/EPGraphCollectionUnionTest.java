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
public class EPGraphCollectionUnionTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphCollectionUnionTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0 2, 2, 2, 5, 8", "0 1, 2 3, 4, 7, 13", "0, 0, 1, 3, 4"})
  public void testUnion(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraphCollection collection1 =
      graphColl.getGraphs(extractGraphIDs(firstColl));
    EPGraphCollection collection2 =
      graphColl.getGraphs(extractGraphIDs(secondColl));

    EPGraphCollection unionColl = collection1.union(collection2);

    assertNotNull("graph collection is null", unionColl);
    assertEquals("wrong number of graphs", expectedCollSize, unionColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      unionColl.getGraph().getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      unionColl.getGraph().getEdgeCount());
  }
}
