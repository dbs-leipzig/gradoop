package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;

import static org.junit.Assert.assertTrue;

public class BinaryCollectionOperatorsTestBase extends GradoopFlinkTestBase {

  protected void checkAssertions(GraphCollection expectation,
    GraphCollection result, String attribute) throws Exception {
    assertTrue(
      "wrong graph ids for " + attribute + " overlapping collections",
      result.equalsByGraphIds(expectation).collect().get(0));
    assertTrue(
      "wrong graph element ids for" + attribute + " overlapping collections",
      result.equalsByGraphElementIds(expectation).collect().get(0));
  }
}
