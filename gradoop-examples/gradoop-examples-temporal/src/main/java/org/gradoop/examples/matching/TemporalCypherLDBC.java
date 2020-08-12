package org.gradoop.examples.matching;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;


public class TemporalCypherLDBC {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Option to declare path to temporal input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option for output path (results are written in a TemporalCSV sink)
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare query
   */
  private static final String OPTION_QUERY = "q";
  /**
   * Option for selectivity
   */
  private static final String OPTION_LOWER = "l";
  /**
   * Option for selectivity
   */
  private static final String OPTION_UPPER = "u";
  /**
   * Indicates whether to count the output embeddings
   */
  private static final String OPTION_SIZE = "s";
  /**
   * Path to serialized binning stats
   */
  private static final String OPTION_BINNING_PATH = "b";
  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used output path for csv statistics
   */
  private static String CSV_PATH;
  /**
   * Used query
   */
  private static String QUERY;
  /**
   * Output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used lower bound
   */
  private static String LOWER;
  /**
   * Used upper bound
   */
  private static String UPPER;
  /**
   * Path to serialized binning
   */
  private static String BINNING_PATH;

  /**
   * count result size?
   */
  private static boolean COUNT;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Input path to indexed source files.");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Output path to csv statistics output");
    OPTIONS.addOption(OPTION_QUERY, "query", true,
      "Used query (q1,q2,q3,q4,q5,q6)");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "outputPath", true,
      "output path for sink");
    OPTIONS.addOption(OPTION_LOWER, "lower", true,
      "lower bound to be used in query (for selectivity)");
    OPTIONS.addOption(OPTION_UPPER, "upper", true,
      "upper bound to be used in query (for selectivity)");
    OPTIONS.addOption(OPTION_SIZE, "output-size", false,
      "print result set size?");
    OPTIONS.addOption(OPTION_BINNING_PATH, "binningPath", true,
      "path to serialized binning stats");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception IO or execution Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args);

    if (cmd == null) {
      return;
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = getExecutionEnvironment();
    TemporalGradoopConfig config = TemporalGradoopConfig.fromGradoopFlinkConfig(
      GradoopFlinkConfig.createConfig(env));

    TemporalGraphStatistics stats = getStats(config);
    // read graph
    TemporalCSVDataSource source = new TemporalCSVDataSource(INPUT_PATH, config);

    TemporalGraph graph = source.getTemporalGraph();

    /*System.out.println("Number of vertices: ");
    System.out.println(graph.getVertices().count());
    long vertCountTime = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);

    System.out.println("vertex count time " + vertCountTime);
    System.out.println(graph.getEdges().count());
    long edgeCountTime = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);
    System.out.println("edge count time " + edgeCountTime);*/

    // prepare collection
    TemporalGraphCollection collection;

    // get cypher query
    String query = QUERY;


    //long statsTime = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);
    //System.out.println("stats time " + statsTime);

    // execute cypher with or without statistics

    collection = graph.query(query, null, stats);

    // write and execute
    TemporalCSVDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, config);
    try {
      collection.writeTo(sink);
      env.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }

    long processTime = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);


    // count embeddings
    //System.out.println(collection.getGraphHeads().count());

    // execute and write job statistics
    if(COUNT){
      long count = collection.getGraphHeads().count();
      System.out.println("count time: "+env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));
      writeCSV(env, processTime, count);
    } else{
      writeCSV(env, processTime);
    }

    System.out.println("query time " + processTime);

  }

  /**
   * Creates the temporal graph
   * @param source DataSource for an EPGM graph
   * @return temporal graph
  private static TemporalGraph getGraph(DataSource source){
  try {
  return toTemporal(source.getLogicalGraph());
  } catch (Exception e) {
  e.printStackTrace();
  }
  return null;
  }*/

  /**
   * Convertes an EPGM graph to a temporal graph
   * @param logicalGraph EPGM graph to convert
   * @return temporal graph
   * @throws Exception if transformation goes wrong
   *//*
  private static TemporalGraph toTemporal(LogicalGraph logicalGraph) throws Exception {
    TemporalGraph tg = TemporalGraph.fromGraph(logicalGraph);
    System.out.println(tg);
    if(GRAPHTYPE.equals("LDBC")){
      tg = tg.transformEdges(new LDBCEdges());
      tg = tg.transformVertices(new LDBCVertices());
      return tg;
    } else{
      return null;
    }
  }*/

  /**
   * Computes or retrieves the statistics for a TPGM graph
   * @param config temporal configuration
   * @return statistics
   */
  private static TemporalGraphStatistics getStats(TemporalGradoopConfig config){
    if(!BINNING_PATH.isEmpty()){
      BinningTemporalGraphStatistics stats = deserializeStats(BINNING_PATH);
      if(stats!= null){
        return stats;
      }
    }
    TemporalCSVDataSource source = new TemporalCSVDataSource(INPUT_PATH, config);
    TemporalGraph g = source.getTemporalGraph();
    return new BinningTemporalGraphStatisticsFactory().fromGraph(g);
  }

  private static BinningTemporalGraphStatistics deserializeStats(String file){
    try{
      FileInputStream fs = new FileInputStream(file);
      ObjectInput in = new ObjectInputStream(fs);
      return (BinningTemporalGraphStatistics) in.readObject();
    } catch(IOException ioe){
      System.out.println("Deserialization failure for "+file);
      System.out.println("Generating stats from graph");
    } catch (ClassNotFoundException e){
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, "output");
    System.out.println("output path: "+OUTPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);

    String queryOption = cmd.getOptionValue(OPTION_QUERY);
    LOWER = cmd.getOptionValue(OPTION_LOWER, null);
    UPPER = cmd.getOptionValue(OPTION_UPPER);

    if(queryOption.equals("q1")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q1(LOWER, UPPER) : TemporalQueriesLDBC.q1();
    } else if(queryOption.equals("q2")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q2(LOWER, UPPER) : TemporalQueriesLDBC.q2();
    } else if(queryOption.equals("q3")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q3(LOWER, UPPER) : TemporalQueriesLDBC.q3();
    } else if(queryOption.equals("q5")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q4(LOWER, UPPER) : TemporalQueriesLDBC.q4();
    } else if(queryOption.equals("q6")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q5(LOWER, UPPER) : TemporalQueriesLDBC.q5();
    } else if(queryOption.equals("q7")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q6(LOWER, UPPER) : TemporalQueriesLDBC.q6();
    } else if(queryOption.equals("q8")){
      QUERY = LOWER!=null ? TemporalQueriesLDBC.q7(LOWER, UPPER) : TemporalQueriesLDBC.q7();
    } else if(queryOption.equals("q5_a_low")){
      QUERY = TemporalQueriesLDBC.q4_a_low();
    } else if(queryOption.equals("q5_a_middle")){
      QUERY = TemporalQueriesLDBC.q4_a_middle();
    } else if(queryOption.equals("q5_a_high")){
      QUERY = TemporalQueriesLDBC.q4_a_high();
    } else if(queryOption.equals("q5_b_low")){
      QUERY = TemporalQueriesLDBC.q4_b_low();
    } else if(queryOption.equals("q5_b_middle")){
      QUERY = TemporalQueriesLDBC.q4_b_middle();
    } else if(queryOption.equals("q5_b_high")){
      QUERY = TemporalQueriesLDBC.q4_b_high();
    } else if(queryOption.equals("q5_c_low")){
      QUERY = TemporalQueriesLDBC.q4_c_low();
    } else if(queryOption.equals("q5_c_middle")){
      QUERY = TemporalQueriesLDBC.q4_c_middle();
    } else if(queryOption.equals("q5_c_high")){
      QUERY = TemporalQueriesLDBC.q4_c_high();
    } else if(queryOption.equals("q9")){
      QUERY = TemporalQueriesLDBC.q8();
    } else if(queryOption.equals("q10")){
      QUERY = TemporalQueriesLDBC.q0();
    }
    COUNT = cmd.hasOption("s");
    System.out.println("Query: "+QUERY);

    if(cmd.hasOption(OPTION_BINNING_PATH)){
      BINNING_PATH = cmd.getOptionValue(OPTION_BINNING_PATH);
    } else{
      BINNING_PATH = "";
    }

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
    if (!cmd.hasOption(OPTION_QUERY)) {
      throw new IllegalArgumentException("Define a query");
    }
    if (cmd.hasOption(OPTION_LOWER) ^ cmd.hasOption(OPTION_UPPER)){
      throw new IllegalArgumentException("Define both lower and upper bound or omit both!");
    }
  }

  protected static CommandLine parseArguments(String[] args)
    throws ParseException {
    return new DefaultParser().parse(OPTIONS, args);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @param runtime the measured runtime
   * @param count cardinality of the result sets
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env, long runtime, long count) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "query",
      "Runtime(s)",
      "Cardinality");

    String tail = String.format("%s|%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
      QUERY,
      runtime,
      count);

    writeToCSV(head, tail);
  }

  private static void writeCSV(ExecutionEnvironment env, long runtime) throws IOException {
    String head = String.format("%s|%s|%s|%s|%s%n",
      "Parallelism",
      "dataset",
      "query",
      "Runtime(s)",
      "Cardinality");

    String tail = String.format("%s|%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH,
      QUERY,
      runtime,
      "NaN");

    writeToCSV(head, tail);
  }

  private static void writeToCSV(String head, String tail) throws IOException {
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
}
