package org.gradoop.model.impl.operators.base;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import static org.junit.Assert.assertTrue;

public class BinaryCollectionOperatorsTestBase extends GradoopFlinkTestBase {

  protected void checkAssertions(
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation,
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result,
    String attribute) throws Exception {
    assertTrue(
      "wrong graph ids for " + attribute + " overlapping collections",
      result.equalsByGraphIds(expectation).collect().get(0));
    assertTrue(
      "wrong graph element ids for" + attribute + " overlapping collections",
      result.equalsByGraphElementIds(expectation).collect().get(0));
  }
}
