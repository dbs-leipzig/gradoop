package org.gradoop.model.impl;

import org.gradoop.model.FlinkTestBase;

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
      differenceColl.size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      differenceColl.getTotalVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      differenceColl.getTotalEdgeCount());
  }

}
