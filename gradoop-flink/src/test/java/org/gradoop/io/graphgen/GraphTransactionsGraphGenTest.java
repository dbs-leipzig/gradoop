package org.gradoop.io.graphgen;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class GraphTransactionsGraphGenTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   * {@link org.gradoop.model.impl.EPGMDatabase#fromGraphGenFile(String, ExecutionEnvironment) fromgraphGenFile}
   * @throws Exception
   */
  @Test
  public void testFromGraphGenFile() throws Exception {
    String graphGenFile =
      EPGMDatabaseGraphGenTest.class.getResource("/data/graphgen/io_test.gg")
        .getFile();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions =
      GraphTransactions.fromGraphGenFile(graphGenFile, getExecutionEnvironment());

    assertEquals("Wrong graph count", 2, graphTransactions.getTransactions().count());

  }
}
