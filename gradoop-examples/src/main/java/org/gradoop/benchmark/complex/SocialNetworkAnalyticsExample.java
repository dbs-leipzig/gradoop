package org.gradoop.benchmark.complex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.benchmark.complex.functions.CountFilter;
import org.gradoop.benchmark.subgraph.SubgraphBenchmark;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsHDFSReader;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program to evaluate a more complex social network analytics example.
 */
public class SocialNetworkAnalyticsExample extends AbstractRunner implements ProgramDescription {
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to declare used statistics
   */
  private static final String OPTION_STATISTICS_PATH = "s";
  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used graph statistics (used for optimization)
   */
  private static String STATISTICS_PATH;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Path to source files.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics");
    OPTIONS.addOption(OPTION_STATISTICS_PATH, "statistics", true,
      "Path to graph statistics");
  }


  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
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

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read logical graph
    DataSource source = new IndexedCSVDataSource(INPUT_PATH, conf);
    LogicalGraph graph = source.getLogicalGraph();

    // create subgraph containing vertices with label: person, tag, country, city and post
    // and its incident edges
    graph = graph.vertexInducedSubgraph(new LabelIsIn<>(
      "person", "tag", "country", "city", "post"));

    // call cypher function (with or without statistics)
    if (cmd.hasOption(OPTION_STATISTICS_PATH)) {
      GraphStatistics statistics = GraphStatisticsHDFSReader
        .read(STATISTICS_PATH, new Configuration());

      graph = graph
        .cypher(getQuery(), getConstruction(), statistics)
        .reduce(new ReduceCombination());

    } else {
      graph = graph
        .cypher(getQuery(), getConstruction())
        .reduce(new ReduceCombination());
    }

    // group on vertex and edge labels + count grouped edges
    LogicalGraph groupedGraph  = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_COMBINE)
      .addVertexGroupingKey("name")
      .useEdgeLabel(true).useVertexLabel(true)
      .addEdgeAggregator(new CountAggregator())
      .build().execute(graph);

    // filter all edges below a fixed threshold
    groupedGraph = groupedGraph.edgeInducedSubgraph(new CountFilter());

    // write data to sink
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(groupedGraph);

    // execute and write job statistics
    env.execute();
    writeCSV(env);
    }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    STATISTICS_PATH = cmd.getOptionValue(OPTION_STATISTICS_PATH);
  }

  /**
   * Returns used cypher query
   *
   * @return used cypher query
   */
  private static String getQuery() {
    return
      "MATCH (p1:person)<-[:hasCreator]-(po:post)<-[:likes]-(p2:person)\n" +
      "(p1)-[:isLocatedIn]->(c1:city)\n" +
      "(p2)-[:isLocatedIn]->(c2:city)" +
      "(po)-[:hasTag]->(t:tag)\n" +
      "(c1)-[:isPartOf]->(ca:country)<-[:isPartOf]-(c2)\n" +
      "WHERE p1 != p2";
  }

  /**
   * Returns used construction pattern
   *
   * @return used construction pattern
   */
  private static String getConstruction() {
    return "CONSTRUCT (ca)-[new:hasInterest]->(t)";
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
    if (!cmd.hasOption(OPTION_STATISTICS_PATH)) {
      throw new IllegalArgumentException("Define a path to generated statistics.");
    }
  }


  /**
   * Method to create and add lines to a csv-file
   *
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String.format("%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "Runtime(s)");

    String tail = String.format("%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
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
        return SocialNetworkAnalyticsExample.class.getName();
    }
}
