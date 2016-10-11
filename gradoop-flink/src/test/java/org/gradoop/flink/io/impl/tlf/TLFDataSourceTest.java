package org.gradoop.flink.io.impl.tlf;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class TLFDataSourceTest extends GradoopFlinkTestBase {
  /**
   * Test method for
   *
   * {@link TLFDataSource#getGraphTransactions()}
   * @throws Exception
   */
  @Test
  public void testRead() throws Exception {
    String tlfFile = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_string.tlf").getFile();

    // create datasource
    DataSource dataSource = new TLFDataSource(tlfFile, config);
    //get transactions
    GraphTransactions transactions = dataSource.getGraphTransactions();

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:B)-[:b]->(v1)]" +
      "g2[(v1:A)-[:a]->(v2:B)<-[:b]-(v1)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    collectAndAssertTrue(
      loader.getGraphCollectionByVariables("g1","g2").equalsByGraphData(
        GraphCollection.fromTransactions(transactions)
      )
    );
  }

  /**
   * Test method for
   *
   * {@link TLFDataSource#getGraphTransactions()}
   * @throws Exception
   */
  @Test
  public void testReadWithDictionary() throws Exception {
    String tlfFile = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test.tlf").getFile();
    String tlfVertexDictionaryFile = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_vertex_dictionary.tlf").getFile();
    String tlfEdgeDictionaryFile = TLFDataSinkTest.class
      .getResource("/data/tlf/io_test_edge_dictionary.tlf").getFile();

    // create datasource
    DataSource dataSource = new TLFDataSource(tlfFile, tlfVertexDictionaryFile,
      tlfEdgeDictionaryFile, config);
    //get transactions
    GraphTransactions transactions = dataSource.getGraphTransactions();

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:B)-[:b]->(v1)]" +
      "g2[(v1:A)-[:a]->(v2:B)<-[:b]-(v1)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    collectAndAssertTrue(
      loader.getGraphCollectionByVariables("g1","g2").equalsByGraphData(
        GraphCollection.fromTransactions(transactions)
      )
    );
  }
}
