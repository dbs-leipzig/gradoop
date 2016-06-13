/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.graphgen;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class GraphTransactionsGraphGenTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link org.gradoop.model.impl.GraphTransactions#fromGraphGenFile(String, ExecutionEnvironment) fromgraphGenFile}
   * @throws Exception
   */
  @Test
  public void testFromGraphGenFile() throws Exception {
    String graphGenFile =
      GraphTransactionsGraphGenTest.class.getResource("/data/graphgen/io_test.gg")
        .getFile();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions =
      GraphTransactions.fromGraphGenFile(graphGenFile, getExecutionEnvironment());

    assertEquals("Wrong graph count", 2, graphTransactions.getTransactions().count());

  }

  /**
   * Test method for
   *
   * {@link org.gradoop.model.impl.GraphTransactions#fromGraphGenFile(String, ExecutionEnvironment) fromgraphGenFile}
   * @throws Exception
   */
  @Test
  public void testFromGraphGenFileWithEdgeCheck() throws Exception {
    String graphGenFile =
      GraphTransactionsGraphGenTest.class.getResource("/data/graphgen/io_test.gg")
        .getFile();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions =
      GraphTransactions.fromGraphGenFile(graphGenFile, getExecutionEnvironment());

    GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo> graphTransaction =
    graphTransactions.getTransactions().collect().get(1);

    assertEquals("Wrong edge count", 2, graphTransaction.getEdges().size());

  }

  /**
   * Test method for
   *
   * {@link org.gradoop.model.impl.GraphTransactions#writeAsGraphGenFile(String) writeAsGraphGenFile}
   */
  @Test
  public void testWriteAsGraphGen() throws Exception {
    String graphGenFileImport =
      GraphTransactionsGraphGenTest.class.getResource
        ("/data/graphgen/io_test.gg")
        .getFile();

    String graphGenFileExport =
      GraphTransactionsGraphGenTest.class.getResource(
        "/data/graphgen")
        .toURI().getPath().toString().concat("/io_test_output");

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions =
      GraphTransactions.fromGraphGenFile(graphGenFileImport, getExecutionEnvironment());

    graphTransactions.writeAsGraphGenFile(graphGenFileExport);


    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
      graphTransactionsCompare = GraphTransactions.fromGraphGenFile
      (graphGenFileExport, getExecutionEnvironment());


    assertEquals("Wrong graph count", 2, graphTransactionsCompare.getTransactions()
      .count());
  }
}
