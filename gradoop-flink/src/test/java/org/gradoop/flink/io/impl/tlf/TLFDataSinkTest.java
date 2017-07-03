
package org.gradoop.flink.io.impl.tlf;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;


public class TLFDataSinkTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link TLFDataSink#write(GraphTransactions)}
   */
  @Test
  public void testWrite() throws Exception {
    String tlfFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test.tlf").getFile();

    String tlfFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile() + "/io_test_output";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport, config);
    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport, getConfig());
    dataSink.write(dataSource.getGraphTransactions(), true);
    // read from output path
    DataSource dataSource2 = new TLFDataSource(tlfFileExport, config);

    getExecutionEnvironment().execute();

    // compare original graph and written one
    collectAndAssertTrue(dataSource.getGraphCollection()
      .equalsByGraphElementData(dataSource2.getGraphCollection()));
  }

  /**
   * Test method for
   *
   * {@link TLFDataSink#write(GraphTransactions)}
   */
  @Test
  public void testWriteWithVertexDictionary() throws Exception {
    String tlfFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test.tlf").getFile();
    String tlfVertexDictionaryFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_vertex_dictionary.tlf").getFile();

    String tlfFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile()+"/io_test_output";

    String tlfVertexDictionaryFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile()
      + "/dictionaries/io_test_output_vertex_dictionary";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport, 
      tlfVertexDictionaryFileImport, "", config);
    // write to output path
    DataSink dataSink = new TLFDataSink(tlfFileExport, 
      tlfVertexDictionaryFileExport, "", getConfig());
    dataSink.write(dataSource.getGraphTransactions(), true);
    // read from output path
    dataSource = new TLFDataSource(tlfFileExport, config);
    GraphTransactions graphTransactions = dataSource.getGraphTransactions();

    // get first transaction which contains one complete graph
    GraphTransaction graphTransaction = graphTransactions
      .getTransactions()
      .collect().get(0);
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
    assertEquals("Wrong graph count", 2,
      graphTransactions.getTransactions().count());
  }

  /**
   * Test method for
   *
   * {@link TLFDataSink#write(GraphTransactions)}
   */
  @Test
  public void testWriteWithDictionaries() throws Exception {
    String tlfFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test.tlf").getFile();
    String tlfVertexDictionaryFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_vertex_dictionary.tlf").getFile();
    String tlfEdgeDictionaryFileImport = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_edge_dictionary.tlf").getFile();

    String tlfFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile() + "/io_test_output";

    String tlfVertexDictionaryFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile()
      + "/dictionaries/io_test_output_vertex_dictionary";

    String tlfEdgeDictionaryFileExport = TLFDataSinkTest.class
      .getResource("/data/tlf").getFile() 
      + "/dictionaries/io_test_output_edge_dictionary";

    // read from inputfile
    DataSource dataSource = new TLFDataSource(tlfFileImport,
      tlfVertexDictionaryFileImport, tlfEdgeDictionaryFileImport, config);
    // write to ouput path
    DataSink dataSink = new TLFDataSink(tlfFileExport,
      tlfVertexDictionaryFileExport, tlfEdgeDictionaryFileExport, getConfig());
    dataSink.write(dataSource.getGraphTransactions(), true);
    // read from output path
    dataSource = new TLFDataSource(tlfFileExport, config);
    GraphTransactions graphTransactions = dataSource.getGraphTransactions();

    //get first transaction which contains one complete graph
    GraphTransaction graphTransaction = graphTransactions
      .getTransactions().collect().get(0);
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
    assertEquals("Wrong graph count", 2, graphTransactions.getTransactions()
      .count());
  }
}
