package org.gradoop.model.impl.algorithms.btgs;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.common.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.VertexPojo;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader = new
      FlinkAsciiGraphLoader<>(getConfig());

    loader.initDatabaseFromFile(
      BusinessTransactionGraphsTest.class
        .getResource("/data/gdl/iig_btgs.gdl").getFile());

    LogicalGraph<GraphHead, VertexPojo, EdgePojo> iig = loader
      .getLogicalGraphByVariable("iig");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("btg1", "btg2", "btg3", "btg4");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> result = iig
      .callForCollection(
        new BusinessTransactionGraphs<GraphHead, VertexPojo, EdgePojo> ());

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}