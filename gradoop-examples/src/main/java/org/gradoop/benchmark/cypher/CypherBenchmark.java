package org.gradoop.benchmark.cypher;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsHDFSReader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

public class CypherBenchmark extends AbstractRunner implements ProgramDescription {
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option do declare path to statistics
   */
  private static final String OPTION_STATISTICS_PATH = "s";
  /**
   * Option to declare path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to declare verification
   */
  private static final String OPTION_QUERY = "q";
  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used to indicate if statistics are used
   */
  private static boolean HAS_STATISTICS;
  /**
   * Used for statistics path
   */
  private static String STATISTICS_INPUT_PATH;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used verification flag
   */
  private static String QUERY;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Path to source files.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics");
    OPTIONS.addOption(OPTION_QUERY, "query", true,
      "Used query (q1,q2,q3,q4,q5,q6)");
    OPTIONS.addOption(OPTION_STATISTICS_PATH, "statistics", true,
      "Path to previously generated statistics.");
  }



  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    DataSource source = new IndexedCSVDataSource(INPUT_PATH, config);

    LogicalGraph graph = source.getLogicalGraph();
    GraphCollection collection;

    String query = getQuery(QUERY);

    if (HAS_STATISTICS) {
      GraphStatistics statistics = GraphStatisticsHDFSReader
        .read(STATISTICS_INPUT_PATH, new org.apache.hadoop.conf.Configuration());

      collection = graph.cypher(query, statistics);
    } else {
      collection = graph.cypher(query);
    }

    DataSink sink = new JSONDataSink(OUTPUT_PATH, config);
    sink.write(collection);

    env.execute();
  }

  private static String getQuery(String query) {
    switch (query) {
    case "q1": return Queries.q1();
    case "q4": return Queries.q4();
    default: throw new IllegalArgumentException("unsupported query: " + query);
    }
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    // read input output paths
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    QUERY = cmd.getOptionValue(OPTION_QUERY);
    HAS_STATISTICS = cmd.hasOption(OPTION_STATISTICS_PATH);
    STATISTICS_INPUT_PATH = cmd.getOptionValue(OPTION_STATISTICS_PATH);
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
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph output directory.");
    }
    if (!cmd.hasOption(OPTION_QUERY)) {
      throw new IllegalArgumentException("Define a query to run (q1,q2,q3,q4,q5,q6).");
    }
  }

  /**
   * Method to create and add lines to a csv-file
   * @throws IOException
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "query",
      "usedStatistics",
      "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
      QUERY,
      HAS_STATISTICS,
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

  @Override
  public String getDescription() {
    return null;
  }
}
