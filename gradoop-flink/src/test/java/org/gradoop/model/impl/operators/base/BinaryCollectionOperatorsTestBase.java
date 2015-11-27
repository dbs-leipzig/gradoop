package org.gradoop.model.impl.operators.base;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BinaryCollectionOperatorsTestBase extends GradoopFlinkTestBase {

  protected void performTest(long expectedCollectionSize,
    long expectedVertexCount, long expectedEdgeCount,
    GraphCollection differenceColl) throws Exception {
    assertNotNull("graph collection is null", differenceColl);
    assertEquals("wrong number of graphs", expectedCollectionSize,
      differenceColl.getGraphCount());
    assertEquals("wrong number of vertices", expectedVertexCount,
      differenceColl.getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      differenceColl.getEdgeCount());
  }

  protected void checkAssertions(
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation,
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result,
    String attribute) throws Exception {
    assertTrue(
      "wrong graph ids for " + attribute + " overlapping collections",
      result.equalsByGraphIdsCollected(expectation));
    assertTrue(
      "wrong graph element ids for" + attribute + " overlapping collections",
      result.equalsByGraphElementIdsCollected(expectation));
  }
}
