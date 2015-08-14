package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.FlinkTest;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class GraphCollectionDifferenceTest extends FlinkTest {
  private EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
    graphStore;

  public GraphCollectionDifferenceTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0 1 2, 0, 2, 5, 8", "0 1, 2 3, 2, 6, 8"})
  public void testDifference(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(extractGraphIDs(firstColl));
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(extractGraphIDs(secondColl));

    GraphCollection differenceColl = collection1.difference(collection2);

    assertNotNull("graph collection is null", differenceColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      differenceColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      differenceColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      differenceColl.getTotalEdgeCount());

    differenceColl = collection1.differenceWithSmallResult(collection2);

    assertNotNull("graph collection is null", differenceColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      differenceColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      differenceColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      differenceColl.getTotalEdgeCount());
  }
}
