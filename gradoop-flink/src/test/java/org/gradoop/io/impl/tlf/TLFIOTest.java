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

package org.gradoop.io.impl.tlf;

import org.gradoop.io.api.DataSink;
import org.gradoop.io.api.DataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;


public class TLFIOTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  /**
   * Test method for
   *
   * {@link TLFDataSource#getGraphTransactions()}
   * @throws Exception
   */
  @Test
  public void testFromTLFFile() throws Exception {
    String tlfFile =
      TLFIOTest.class
        .getResource("/data/tlf/io_test.tlf")
        .getFile();

    // create datasource
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(tlfFile, config);
    //get transactions
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions
      = dataSource.getGraphTransactions();

    assertEquals("Wrong graph count", 2, graphTransactions.getTransactions()
      .count());

  }

  /**
   * Test method for
   *
   * {@link TLFDataSource#getGraphTransactions()}
   * @throws Exception
   */
  @Test
  public void testFromTLFFileWithEdgeCheck() throws Exception {
    String tlfFile =
      TLFIOTest.class
        .getResource("/data/tlf/io_test.tlf")
        .getFile();
    // create datasource
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(tlfFile, config);
    //get transactions
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions
      = dataSource.getGraphTransactions();
    //get first transaction which contains one complete graph
    GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo> graphTransaction =
    graphTransactions.getTransactions().collect().get(1);

    assertEquals("Wrong edge count", 2, graphTransaction.getEdges().size());

  }

  /**
   * Test method for
   *
   * {@link TLFDataSink#write(GraphTransactions)}
   */
  @Test
  public void testWriteAsTLF() throws Exception {
    String tmpDir = temporaryFolder.getRoot().toString();

    String tlfFileImport =
      TLFIOTest.class.getResource
        ("/data/tlf/io_test.tlf")
        .getFile();

    String tlfFileExport =
      TLFIOTest.class.getResource("/data/tlf")
        .toURI().getPath().concat("/io_test_output");
    File file = new File(tlfFileExport);
    if (!file.exists()) {
      file.createNewFile();
    }

    // read from inputfile
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TLFDataSource<>(tlfFileImport, config);
    // write to ouput path
    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new TLFDataSink<>(tlfFileExport,getConfig());
    dataSink.write(dataSource.getGraphTransactions());
    // read from output path
    dataSource = new TLFDataSource<>(tlfFileExport, config);
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> graphTransactions
      = dataSource.getGraphTransactions();

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 2, graphTransactions.getTransactions()
      .count());
  }
}
