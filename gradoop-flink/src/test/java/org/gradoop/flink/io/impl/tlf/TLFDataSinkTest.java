/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.tlf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;


public class TLFDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tlfFileImport = getFilePath("/data/tlf/io_test.tlf");

    String tlfFileExport = getFilePath("/data/tlf") + "/io_test_output";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport, getConfig());
    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport, getConfig());
    dataSink.write(dataSource.getGraphCollection(), true);
    // read from output path
    DataSource dataSource2 = new TLFDataSource(tlfFileExport, getConfig());

    getExecutionEnvironment().execute();

    // compare original graph and written one
    collectAndAssertTrue(dataSource.getGraphCollection()
      .equalsByGraphElementData(dataSource2.getGraphCollection()));
  }

  @Test
  public void testWriteWithoutEdges() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    
    String tlfFileImport = getFilePath("/data/tlf/io_test_string_without_edges.tlf");

    String tlfFileExport = tmpPath + "/data/tlf/io_test_output";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport, getConfig());
    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport, getConfig());
    dataSink.write(dataSource.getGraphCollection(), true);
    // read from output path
    DataSource dataSource2 = new TLFDataSource(tlfFileExport, getConfig());

    getExecutionEnvironment().execute();

    // compare original graph and written one
    collectAndAssertTrue(dataSource.getGraphCollection()
    .equalsByGraphElementData(dataSource2.getGraphCollection()));
  }

  @Test
  public void testWriteWithoutEdgesWithDictionaries() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    
    String tlfFileImport = getFilePath("/data/tlf/io_test_string_without_edges.tlf");

    String tlfFileExport = tmpPath + "/data/tlf/io_test_output";

    String tlfVertexDictionaryFileExport =
      tmpPath + "/data/tlf/dictionaries/io_test_output_vertex_dictionary";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport, getConfig());
    GraphCollection input = dataSource.getGraphCollection();

    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport, tlfVertexDictionaryFileExport,
      "", getConfig());
    dataSink.write(input, true);

    // read from output path
    DataSource dataSource2 = new TLFDataSource(tlfFileExport, tlfVertexDictionaryFileExport,
      "", getConfig());
    GraphCollection output = dataSource2.getGraphCollection();

    getExecutionEnvironment().execute();

    // compare original graph and written one
    collectAndAssertTrue(input.equalsByGraphElementData(output));
  }

  @Test
  public void testWriteWithVertexDictionary() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    
    String tlfFileImport = getFilePath("/data/tlf/io_test.tlf");
    String tlfVertexDictionaryFileImport = getFilePath("/data/tlf/io_test_vertex_dictionary.tlf");

    String tlfFileExport = getFilePath("/data/tlf") + "/io_test_output";

    String tlfVertexDictionaryFileExport =
      tmpPath + "/data/tlf/dictionaries/io_test_output_vertex_dictionary";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport,
      tlfVertexDictionaryFileImport, "", getConfig());
    // write to output path
    DataSink dataSink = new TLFDataSink(tlfFileExport,
      tlfVertexDictionaryFileExport, "", getConfig());
    dataSink.write(dataSource.getGraphCollection(), true);
    // read from output path
    dataSource = new TLFDataSource(tlfFileExport, getConfig());
    DataSet<GraphTransaction> graphTransactions = dataSource
      .getGraphCollection()
      .getGraphTransactions();

    // get first transaction which contains one complete graph
    GraphTransaction graphTransaction = graphTransactions.collect().get(0);
    // get vertices of the first transaction/graph
    EPGMVertex[] vertexArray = graphTransaction.getVertices().toArray(
      new EPGMVertex[graphTransaction.getVertices().size()]);
    // sort vertices by label(alphabetically)
    Arrays.sort(vertexArray, new Comparator<EPGMVertex>() {
      @Override
      public int compare(EPGMVertex vertex1, EPGMVertex vertex2) {
        return vertex1.getLabel().compareTo(vertex2.getLabel());
      }
    });

    assertEquals("Wrong vertex label", "0", vertexArray[0].getLabel());
    assertEquals("Wrong vertex label", "1", vertexArray[1].getLabel());
    assertEquals("Wrong graph count", 2, graphTransactions.count());
  }

  @Test
  public void testWriteWithDictionaries() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();
    
    String tlfFileImport = getFilePath("/data/tlf/io_test.tlf");
    String tlfVertexDictionaryFileImport = getFilePath("/data/tlf/io_test_vertex_dictionary.tlf");
    String tlfEdgeDictionaryFileImport = getFilePath("/data/tlf/io_test_edge_dictionary.tlf");

    String tlfFileExport = getFilePath("/data/tlf") + "/io_test_output";

    String tlfVertexDictionaryFileExport =
      tmpPath + "/data/tlf/dictionaries/io_test_output_vertex_dictionary";

    String tlfEdgeDictionaryFileExport =
        getFilePath("/data/tlf") + "/dictionaries/io_test_output_edge_dictionary";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport,
      tlfVertexDictionaryFileImport, tlfEdgeDictionaryFileImport, getConfig());
    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport,
      tlfVertexDictionaryFileExport, tlfEdgeDictionaryFileExport, getConfig());
    dataSink.write(dataSource.getGraphCollection(), true);
    // read from output path
    dataSource = new TLFDataSource(tlfFileExport, getConfig());
    DataSet<GraphTransaction> graphTransactions = dataSource.getGraphCollection().getGraphTransactions();

    //get first transaction which contains one complete graph
    GraphTransaction graphTransaction = graphTransactions.collect().get(0);
    //get vertices of the first transaction/graph
    EPGMVertex[] vertexArray = graphTransaction.getVertices().toArray(
      new EPGMVertex[graphTransaction.getVertices().size()]);
    //sort vertices by label(alphabetically)
    Arrays.sort(vertexArray, new Comparator<EPGMVertex>() {
      @Override
      public int compare(EPGMVertex vertex1, EPGMVertex vertex2) {
        return vertex1.getLabel().compareTo(vertex2.getLabel());
      }
    });

    assertEquals("Wrong vertex label", "0", vertexArray[0].getLabel());
    assertEquals("Wrong vertex label", "1", vertexArray[1].getLabel());
    assertEquals("Wrong graph count", 2, graphTransactions.count());
  }
}
