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

  /**
   * Test method for
   * {@link org.gradoop.model.impl.EPGMDatabase#fromGraphGenFile(String, ExecutionEnvironment) fromgraphGenFile}
   * @throws Exception
   */
  @Test
  public void testFromGraphGenFileWithEdgeCheck() throws Exception {
    String graphGenFile =
      EPGMDatabaseGraphGenTest.class.getResource("/data/graphgen/io_test.gg")
        .getFile();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions =
      GraphTransactions.fromGraphGenFile(graphGenFile, getExecutionEnvironment());

    GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo> graphTransaction =
    graphTransactions.getTransactions().collect().get(1);

    assertEquals("Wrong edge count", 2, graphTransaction.getEdges().size());

  }
}
