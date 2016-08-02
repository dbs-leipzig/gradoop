package org.gradoop.model.impl.algorithms.btgs;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    FlinkAsciiGraphLoader<GraphHead, Vertex, Edge> loader = new
      FlinkAsciiGraphLoader<>(getConfig());

    loader.initDatabaseFromFile(
      BusinessTransactionGraphsTest.class
        .getResource("/data/gdl/iig_btgs.gdl").getFile());

    LogicalGraph<GraphHead, Vertex, Edge> iig = loader
      .getLogicalGraphByVariable("iig");

    GraphCollection<GraphHead, Vertex, Edge> expectation =
      loader.getGraphCollectionByVariables("btg1", "btg2", "btg3", "btg4");

    GraphCollection<GraphHead, Vertex, Edge> result = iig
      .callForCollection(
        new BusinessTransactionGraphs<GraphHead, Vertex, Edge> ());

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}