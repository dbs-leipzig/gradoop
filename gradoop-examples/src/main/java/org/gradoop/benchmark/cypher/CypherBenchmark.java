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
package org.gradoop.benchmark.cypher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.benchmark.subgraph.SubgraphBenchmark;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsHDFSReader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized graph cypher benchmark.
 */
public class CypherBenchmark extends AbstractRunner implements ProgramDescription {
  /**
   * Option to declare path to input graph. Using a store, the path is the table prefix without '.'.
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare input graph format (csv, indexed, hbase)
   */
  private static final String OPTION_INPUT_FORMAT = "f";
  /**
   * Option do declare path to statistics
   */
  private static final String OPTION_STATISTICS_PATH = "s";
  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to declare verification
   */
  private static final String OPTION_QUERY = "q";
  /**
   * Option for used first name in query.
   */
  private static final String OPTION_FIRST_NAME = "n";
  /**
   * Option to enable predicate pushdown for store input.
   */
  private static final String OPTION_USE_PREDICATE_PUSHDOWN = "r";

  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used input format (csv, indexed, hbase)
   */
  private static String INPUT_FORMAT;
  /**
   * Used to indicate if statistics are used
   */
  private static boolean HAS_STATISTICS;
  /**
   * Used statistics input path
   */
  private static String STATISTICS_INPUT_PATH;
  /**
   * Used output path for csv statistics
   */
  private static String CSV_PATH;
  /**
   * Used query
   */
  private static String QUERY;
  /**
   * Used first name for query (q1,q2,q3)
   */
  private static String FIRST_NAME;
  /**
   * Used to indicate the usage of predicate pushdown
   */
  private static boolean USE_PREDICATE_PUSHDOWN;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Input path to source files or table prefix of store.");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Input graph format (csv, indexed, hbase).");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Output path to csv statistics output");
    OPTIONS.addOption(OPTION_QUERY, "query", true,
      "Used query (q1,q2,q3,q4,q5,q6)");
    OPTIONS.addOption(OPTION_FIRST_NAME, "query-name", true,
      "Used first Name in Cypher Query");
    OPTIONS.addOption(OPTION_STATISTICS_PATH, "statistics", true,
      "Input path to previously generated statistics.");
    OPTIONS.addOption(OPTION_USE_PREDICATE_PUSHDOWN, "predicatepushdown", false,
      "Flag to use predicate pushdown for store.");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments, e.g.:
   *             -i hdfs:///mygraph -f indexed -c /home/user/result.csv -q q1 -n "Carlos"
   *             -s hdfs:///mygraph/stats
   * @throws Exception IO or execution Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SubgraphBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source = getDataSource(INPUT_PATH, INPUT_FORMAT, config);

    // apply predicates to store sources if pushdown is enabled
    if (USE_PREDICATE_PUSHDOWN) {
      switch (INPUT_FORMAT) {
      case FORMAT_HBASE:
        source = ((HBaseDataSource) source)
          .applyVertexPredicate(Predicates.HBase.getVertexFilter(QUERY, FIRST_NAME));
        source = ((HBaseDataSource) source)
          .applyEdgePredicate(Predicates.HBase.getEdgeFilter(QUERY));
        break;

      case FORMAT_ACCUMULO:
        source = ((AccumuloDataSource) source)
          .applyVertexPredicate(Predicates.Accumulo.getVertexFilter(QUERY, FIRST_NAME));
        source = ((AccumuloDataSource) source)
          .applyEdgePredicate(Predicates.Accumulo.getEdgeFilter(QUERY));
        break;
      }
    }

    LogicalGraph graph = source.getLogicalGraph();

    // prepare collection
    GraphCollection collection;

    // get cypher query
    String query = getQuery(QUERY);

    // execute cypher with or without statistics
    if (HAS_STATISTICS) {
      GraphStatistics statistics = GraphStatisticsHDFSReader
        .read(STATISTICS_INPUT_PATH, new Configuration());

      collection = graph.query(query, statistics);
    } else {
      collection = graph.query(query);
    }

    // count embeddings
    System.out.println(collection.getGraphHeads().count());

    // execute and write job statistics
    writeCSV(env);
  }

  /**
   * Returns used query for benchmark
   *
   * @param query argument input
   * @return used query
   */
  private static String getQuery(String query) {
    switch (query) {
    case "q1" : return Queries.q1(FIRST_NAME);
    case "q2" : return Queries.q2(FIRST_NAME);
    case "q3" : return Queries.q3(FIRST_NAME);
    case "q4" : return Queries.q4();
    case "q5" : return Queries.q5();
    case "q6" : return Queries.q6();
    default : throw new IllegalArgumentException("Unsupported query: " + query);
    }
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    QUERY = cmd.getOptionValue(OPTION_QUERY);
    HAS_STATISTICS = cmd.hasOption(OPTION_STATISTICS_PATH);
    STATISTICS_INPUT_PATH = cmd.getOptionValue(OPTION_STATISTICS_PATH);
    FIRST_NAME = cmd.getOptionValue(OPTION_FIRST_NAME);
    USE_PREDICATE_PUSHDOWN = cmd.hasOption(OPTION_USE_PREDICATE_PUSHDOWN);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_INPUT_FORMAT)) {
      throw new IllegalArgumentException("Define a graph input format.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!cmd.hasOption(OPTION_QUERY)) {
      throw new IllegalArgumentException("Define a query to run (q1,q2,q3,q4,q5,q6).");
    }
    if (cmd.getOptionValue(OPTION_QUERY).equals("q1") ||
        cmd.getOptionValue(OPTION_QUERY).equals("q2") ||
        cmd.getOptionValue(OPTION_QUERY).equals("q3")) {
      if (!cmd.hasOption(OPTION_FIRST_NAME)) {
        throw new IllegalArgumentException("Define a first name for query");
      }
    }
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "format",
      "query",
      "usedStatistics",
      "predPushdown",
      "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
      INPUT_FORMAT,
      QUERY,
      HAS_STATISTICS,
      USE_PREDICATE_PUSHDOWN,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return CypherBenchmark.class.getName();
  }
}
