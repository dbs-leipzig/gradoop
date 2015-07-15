package org.gradoop.model.impl;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class EPGraphCollectionGetGraphTest extends EPFlinkTest {

  private EPGraphStore graphStore;

  public EPGraphCollectionGetGraphTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0, Community, 3, 4", "1, Community, 3, 4", "2, Community, 4, 6",
    "3, Forum, 3, 4"})
  public void testGetGraph(long graphID, String expectedGraphLabel,
    long expectedVertexCount, long expectedEdgeCount) throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();

    EPGraph g = graphColl.getGraph(graphID);
    assertNotNull("graph was null", g);
    assertEquals("vertex set has the wrong size", expectedVertexCount,
      g.getVertices().size());
    assertEquals("edge set has the wrong size", expectedEdgeCount,
      g.getEdges().size());
    assertEquals("wrong label", expectedGraphLabel, g.getLabel());
  }

  @Test
  @Parameters({"0 1, 6, 8", "0 3, 6, 8", "0 1 2, 6, 10", "1 3, 4, 7"})
  public void testGetGraphs(String graphIDString, long expectedVertexCount,
    long expectedEdgeCount) throws Exception {
    EPGraphCollection graphColl = graphStore.getCollection();

    List<Long> graphIDs = extractGraphIDs(graphIDString);
    EPGraphCollection graphs = graphColl.getGraphs(graphIDs);

    assertNotNull("graph collection is null", graphs);
    assertEquals("wrong number of graphs", graphIDs.size(), graphs.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      graphs.getGraph().getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      graphs.getGraph().getEdgeCount());
  }
}
