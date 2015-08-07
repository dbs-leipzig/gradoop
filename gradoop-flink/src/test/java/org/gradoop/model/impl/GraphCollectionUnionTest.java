package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.FlinkTest;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class GraphCollectionUnionTest extends FlinkTest {
  private EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
    graphStore;

  public GraphCollectionUnionTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0 2, 2, 2, 5, 8", "0 1, 2 3, 4, 7, 13", "0, 0, 1, 3, 4"})
  public void testUnion(String firstColl, String secondColl,
    long expectedCollSize, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(extractGraphIDs(firstColl));
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(extractGraphIDs(secondColl));

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      unionColl = collection1.union(collection2);

    assertNotNull("graph collection is null", unionColl);
    assertEquals("wrong number of graphs", expectedCollSize, unionColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      unionColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      unionColl.getTotalEdgeCount());
  }
}
