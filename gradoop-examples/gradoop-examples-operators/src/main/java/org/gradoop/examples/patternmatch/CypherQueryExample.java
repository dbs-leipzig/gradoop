package org.gradoop.examples.patternmatch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.common.SocialNetworkGraph;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * A self-contained example on how to use the query engine in Gradoop.
 *
 * The example uses the graph in dev-support/social-network.pdf
 */
public class CypherQueryExample {

  /**
   * Runs the program on the example data graph.
   *
   * The example provides an overview over the usage of the cypher() method.
   * It showcases how a user defined (cypher) query can be applied to a graph.
   * Documentation and usage examples can be found in the projects wiki.
   *
   * Using the social network graph  {@link SocialNetworkGraph}, the program will:
   * 1. create the graph based on the given gdl string
   * 2. run the query method with a user defined (cypher) query string.
   * 3. print all found matches
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
    loader.initDatabaseFromString(
      URLDecoder.decode(SocialNetworkGraph.getGraphGDLString(), StandardCharsets.UTF_8.name()));

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
    matches.getGraphHeads().print();
    matches.getVertices().print();
    matches.getEdges().print();
  }
}
