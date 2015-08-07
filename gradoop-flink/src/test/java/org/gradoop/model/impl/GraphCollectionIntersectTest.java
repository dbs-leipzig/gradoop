package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.FlinkTest;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class GraphCollectionIntersectTest extends FlinkTest {

  private EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
    graphStore;

  public GraphCollectionIntersectTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({
    "0 1 2, 0 1, 2, 6, 10", "0, 1, 0, 0, 0", "0 2 3, 1 2 3, 2, 5, 9"
  })
  public void testIntersect(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(extractGraphIDs(firstColl));
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(extractGraphIDs(secondColl));

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      intersectColl = collection1.intersect(collection2);

    assertNotNull("graph collection is null", intersectColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      intersectColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      intersectColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      intersectColl.getTotalEdgeCount());

    intersectColl = collection1.intersectWithSmall(collection2);

    assertNotNull("graph collection is null", intersectColl);
    assertEquals("wrong number of graphs", expectedCollSize,
      intersectColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      intersectColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      intersectColl.getTotalEdgeCount());
  }
}
