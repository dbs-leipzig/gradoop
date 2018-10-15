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
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class TLFDataSourceTest extends GradoopFlinkTestBase {
  @Test
  public void testRead() throws Exception {
    String tlfFile = getFilePath("/data/tlf/io_test_string.tlf");

    // create datasource
    DataSource dataSource = new TLFDataSource(tlfFile, getConfig());
    // get transactions
    DataSet<GraphTransaction> transactions = dataSource.getGraphCollection().getGraphTransactions();

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:B)-[:b]->(v1)]" +
      "g2[(v1:A)-[:a]->(v2:B)<-[:b]-(v1)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    collectAndAssertTrue(
      loader.getGraphCollectionByVariables("g1","g2").equalsByGraphData(
        getConfig().getGraphCollectionFactory().fromTransactions(transactions)
      )
    );
  }

  @Test
  public void testReadWithoutEdges() throws Exception {
    String tlfFile = getFilePath("/data/tlf/io_test_string_without_edges.tlf");

    // create datasource
    DataSource dataSource = new TLFDataSource(tlfFile, getConfig());
    // get transactions
    DataSet<GraphTransaction> transactions = dataSource.getGraphCollection().getGraphTransactions();

    String asciiGraphs = "" +
      "g1[(v1:A),(v2:B)]" +
      "g2[(v1:A),(v2:B)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    collectAndAssertTrue(
    loader.getGraphCollectionByVariables("g1","g2").equalsByGraphData(
    getConfig().getGraphCollectionFactory().fromTransactions(transactions)
    )
    );
  }

  @Test
  public void testReadWithDictionary() throws Exception {
    String tlfFile = getFilePath("/data/tlf/io_test.tlf");
    String tlfVertexDictionaryFile = getFilePath("/data/tlf/io_test_vertex_dictionary.tlf");
    String tlfEdgeDictionaryFile = getFilePath("/data/tlf/io_test_edge_dictionary.tlf");

    // create datasource
    DataSource dataSource = new TLFDataSource(tlfFile, tlfVertexDictionaryFile,
      tlfEdgeDictionaryFile, getConfig());
    // get transactions
    DataSet<GraphTransaction> transactions = dataSource.getGraphCollection().getGraphTransactions();

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:B)-[:b]->(v1)]" +
      "g2[(v1:A)-[:a]->(v2:B)<-[:b]-(v1)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    collectAndAssertTrue(
      loader.getGraphCollectionByVariables("g1","g2").equalsByGraphData(
        getConfig().getGraphCollectionFactory().fromTransactions(transactions)
      )
    );
  }
}
