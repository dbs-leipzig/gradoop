package org.gradoop.examples.patternmatch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class CypherPatternMatchExample {
  /**
   * Path to the data graph.
   */
  private static final String DATA_PATH = GDLPatternMatchExample.class.getResource("/data/csv/sna").getFile();

  public static void main(String[] args) throws Exception {
    // initialize Apache Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // create a Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    // create a datasource
    CSVDataSource csvDataSource = new CSVDataSource(
      URLDecoder.decode(DATA_PATH, StandardCharsets.UTF_8.name()), config);

    // load graph from datasource (lazy)
    LogicalGraph socialNetwork = csvDataSource.getLogicalGraph();

    // run a Cypher query (vertex homomorphism, edge isomorphism)
    // the result is a graph collection containing all matching subgraphs
    GraphCollection matches = socialNetwork.cypher(
      "MATCH (u1:Person)<-[:hasModerator]-(f:Forum)" +
        "(u2:Person)<-[:hasMember]-(f)" +
        "WHERE u1.name = \"Alice\"").getGraphs();


    // Print the graph to system out
    // alternatively, one can use a org.gradoop.flink.io.api.DataSink to store the whole collection
    // or use the result in subsequent analytical steps
    matches.print();
  }
}
