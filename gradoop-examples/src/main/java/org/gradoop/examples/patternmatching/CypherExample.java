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
package org.gradoop.examples.patternmatching;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self-contained example on how to use the Cypher query engine in Gradoop.
 *
 * The example uses the graph in dev-support/social-network.pdf
 */
public class CypherExample {
  /**
   * Path to the data graph.
   */
  static final String DATA_PATH = CypherExample.class.getResource("/data/csv/sna").getFile();
  /**
   * Path to the data graph statistics (computed using {@link org.gradoop.utils.statistics.StatisticsRunner}
   */
  static final String STATISTICS_PATH = DATA_PATH + "/statistics";

  /**
   * Runs the example program on the toy graph.
   *
   * @param args arguments
   * @throws Exception in case sth goes wrong
   */
  public static void main(String[] args) throws Exception {
    // initialize Apache Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create a Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    // create a datasource
    CSVDataSource csvDataSource = new CSVDataSource(
      URLDecoder.decode(DATA_PATH, StandardCharsets.UTF_8.name()), config);
    // load graph statistics
    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(STATISTICS_PATH);

    // load graph from datasource (lazy)
    LogicalGraph socialNetwork = csvDataSource.getLogicalGraph();

    // run a Cypher query (vertex homomorphism, edge isomorphism)
    // the result is a graph collection containing all matching subgraphs
    GraphCollection matches = socialNetwork.query(
      "MATCH (u1:Person)<-[:hasModerator]-(f:Forum)" +
      "(u2:Person)<-[:hasMember]-(f)" +
      "WHERE u1.name = \"Alice\"", statistics);

    // Print the graph to system out
    // alternatively, one can use a org.gradoop.flink.io.api.DataSink to store the whole collection
    // or use the result in subsequent analytical steps
    matches.print();
  }
}
