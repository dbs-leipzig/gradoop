package org.gradoop.model.impl.operators;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BinaryCollectionOperatorsTestBase extends FlinkTestBase {

  public BinaryCollectionOperatorsTestBase(TestExecutionMode mode) {
    super(mode);
  }

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

}
