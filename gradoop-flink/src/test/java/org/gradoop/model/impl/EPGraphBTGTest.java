package org.gradoop.model.impl;

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.impl.operators.BTG;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphBTGTest extends EPFlinkTest {
  private EPGraph epGraph;

  public EPGraphBTGTest() {
    String vertexFile =
      FlinkGraphStoreTest.class.getResource("/btg/btg_nodes").getFile();
    String edgeFile =
      FlinkGraphStoreTest.class.getResource("/btg/btg_edges").getFile();
    EPGraphStore graphStore =
      FlinkGraphStore.fromJsonFile(vertexFile, edgeFile, null, env);
    this.epGraph = graphStore.getDatabaseGraph();
  }

  @Test
  public void testBTGWithCallByPropertyKey() throws Exception {
    EPGraphCollection btgGraph = epGraph.callForCollection(new BTG(50, env));
    btgGraph.getGellyGraph().getVertices().print();
    assertNotNull("graph collection is null", epGraph);
    assertEquals("wrong number of graphs", 2l, btgGraph.size());
    assertEquals("wrong number of vertices", 16l,
      btgGraph.getGraph().getVertexCount());
    assertEquals("wrong number of edges", 46l,
      btgGraph.getGraph().getEdgeCount());
  }
}
