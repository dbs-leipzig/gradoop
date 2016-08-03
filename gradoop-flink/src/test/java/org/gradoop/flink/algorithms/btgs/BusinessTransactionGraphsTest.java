package org.gradoop.flink.algorithms.btgs;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(
      BusinessTransactionGraphsTest.class
        .getResource("/data/gdl/iig_btgs.gdl").getFile());

    LogicalGraph iig = loader.getLogicalGraphByVariable("iig");

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("btg1", "btg2", "btg3", "btg4");

    GraphCollection result = iig
      .callForCollection(new BusinessTransactionGraphs());

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}