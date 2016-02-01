package org.gradoop.model.impl.algorithms.btgs;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader = new
      FlinkAsciiGraphLoader<>(getConfig());

    loader.initDatabaseFromFile(
      BusinessTransactionGraphsTest.class
        .getResource("/data/gdl/iig_btgs.gdl").getFile());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> iig = loader
      .getLogicalGraphByVariable("iig");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("btg1", "btg2", "btg3", "btg4");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result = iig
      .callForCollection(
        new BusinessTransactionGraphs<GraphHeadPojo, VertexPojo, EdgePojo> ());

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}