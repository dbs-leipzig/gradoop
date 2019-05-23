/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.examples.patternmatch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A self-contained example on how to use the query engine in Gradoop.
 *
 * The example uses the graph in dev-support/social-network.pdf
 */
public class CypherQueryExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the {@link LogicalGraph#cypher(String)}
   * method. It showcases how a user defined (cypher) query can be applied to a graph.
   * Documentation and usage examples can be found in the projects wiki.
   *
   * Using the social network graph  {@link SocialNetworkGraph}, the program will:
   * <ol>
   *   <li>create the graph based on the given gdl string</li>
   *   <li>run the query method with a user defined (cypher) query string</li>
   *   <li>print all found matches</li>
   * </ol>
   *
   * @param args no arguments provided
   * @see <a href="https://github.com/dbs-leipzig/gradoop/wiki/Unary-Logical-Graph-Operators">
   * Gradoop Wiki</a>
   * @throws Exception in case sth goes wrong
   */
  public static void main(String[] args) throws Exception {
    // create flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(GradoopFlinkConfig.createConfig(env));

    // load data
    loader.initDatabaseFromString(SocialNetworkGraph.getGraphGDLString());

    // load graph
    LogicalGraph socialNetwork = loader.getLogicalGraph();

    // run a Cypher query (vertex homomorphism, edge isomorphism)
    // the result is a graph collection containing all matching subgraphs
    GraphCollection matches = socialNetwork.cypher(
      "MATCH (u1:Person)<-[:hasModerator]-(f:Forum)," +
            "(u2:Person)<-[:hasMember]-(f)" +
      "WHERE u1.name = \"Alice\"" +
      "RETURN *").getGraphs();

    // print found matches
    matches.print();
  }
}
