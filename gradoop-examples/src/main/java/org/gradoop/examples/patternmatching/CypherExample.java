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

package org.gradoop.examples.patternmatching;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
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
  static final String DATA_PATH = CypherExample.class.getResource("/data/json/sna").getPath();
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
    DataSource jsonDataSource = new JSONDataSource(DATA_PATH, config);
    // load graph statistics
    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(STATISTICS_PATH);

    // load graph from datasource (lazy)
    LogicalGraph socialNetwork = jsonDataSource.getLogicalGraph();

    // run a Cypher query (vertex homomorphism, edge isomorphism)
    // the result is a graph collection containing all matching subgraphs
    GraphCollection matches = socialNetwork.cypher(
      "MATCH (u1:Person)<-[:hasModerator]-(f:Forum)" +
      "(u2:Person)<-[:hasMember]-(f)" +
      "WHERE u1.name = \"Alice\"", statistics);

    // this just prints the graph heads to system out
    // alternatively, one can use a org.gradoop.flink.io.api.DataSink to store the whole collection
    // or use the result in subsequent analytical steps
    matches.getGraphHeads().print();
  }
}
