package org.gradoop.flink.model.impl.operators.base;

import org.gradoop.flink.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;

import static org.junit.Assert.assertTrue;

public abstract class ReducibleBinaryOperatorsTestBase extends BinaryGraphOperatorsTestBase {

  protected void checkExpectationsEqualResults(FlinkAsciiGraphLoader loader,
    UnaryCollectionToGraphOperator operator) throws Exception {
    // overlap
    GraphCollection col13 = loader.getGraphCollectionByVariables("g1", "g3");

    LogicalGraph exp13 = loader.getLogicalGraphByVariable("exp13");

    // no overlap
    GraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    LogicalGraph exp12 = loader.getLogicalGraphByVariable("exp12");

    // full overlap
    GraphCollection col14 = loader.getGraphCollectionByVariables("g1", "g4");

    LogicalGraph exp14 = loader.getLogicalGraphByVariable("exp14");

    assertTrue("partial overlap failed",
      operator.execute(col13).equalsByElementData(exp13).collect().get(0));
    assertTrue("without overlap failed",
      operator.execute(col12).equalsByElementData(exp12).collect().get(0));
    assertTrue("with full overlap failed",
      operator.execute(col14).equalsByElementData(exp14).collect().get(0));
  }
}
