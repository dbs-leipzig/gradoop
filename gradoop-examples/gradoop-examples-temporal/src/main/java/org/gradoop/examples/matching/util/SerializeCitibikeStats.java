package org.gradoop.examples.matching.util;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.gradoop.examples.matching.TemporalQueries;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.ReservoirSampler;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
/**
 * Class to create a {@link BinningTemporalGraphStatistics}
 * for a temporal graph in advance (to reuse it for different queries)
 */
public class SerializeCitibikeStats {
  /**
   * Command line options for the runner.
   */
  protected static final Options OPTIONS = new Options();
  /**
   * Option to declare path to temporal input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option for output path
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option for reservoir sample size (default 5000)
   */
  private static final String OPTION_SAMPLE_SIZE = "s";
  /**
   * Input path
   */
  private static String INPUT_PATH;
  /**
   * Output path
   */
  private static String OUTPUT_PATH;
  /**
   * reservoir sample size (default 5000)
   */
  private static int SAMPLE_SIZE = ReservoirSampler.DEFAULT_SAMPLE_SIZE;
  /**
   * numerical properties to consider
   */
  private static HashSet<String> numericalProperties = new HashSet<>();
  /**
   * categorical properties to consider
   */
  private static HashSet<String> categoricalProperties = new HashSet<>();

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Input path to indexed source files.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "outputPath", true,
      "output path for sink");
    OPTIONS.addOption(OPTION_SAMPLE_SIZE, "sampleSize", true,
      "size of reservoir samples");
    ArrayList<String> categorical = new ArrayList<>(Arrays.asList("id", "name", "bike_id", "gender",
      "user_type", "year_birth"));

    categoricalProperties.addAll(categorical);
  }

  public static void main(String[] args) throws Exception{
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

    // create stats for input graph
    TemporalCSVDataSource source = new TemporalCSVDataSource(INPUT_PATH, config);
    TemporalGraph g = source.getTemporalGraph();
    BinningTemporalGraphStatistics stats =
      new BinningTemporalGraphStatisticsFactory().fromGraph(g, numericalProperties, categoricalProperties);


    // serialize it
    String filename = OUTPUT_PATH+"/stats.ser";
    try{
      FileOutputStream fs = new FileOutputStream(filename);
      ObjectOutputStream out = new ObjectOutputStream(fs);
      out.writeObject(stats);
    } catch (Exception e){
      System.out.println("Serialization failed: ");
      e.printStackTrace();
      System.exit(-1);
    }
    // write textual representation to file
    String textualFile = OUTPUT_PATH+"/stats_textual.txt";
    try{
      FileOutputStream out = new FileOutputStream(textualFile);
      out.write(stats.toString().getBytes());
      out.close();
    } catch(IOException ioe){
      ioe.printStackTrace();
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
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a statistics output directory.");
    }
  }

  protected static CommandLine parseArguments(String[] args)
    throws ParseException {
    return new DefaultParser().parse(OPTIONS, args);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, INPUT_PATH);
    //System.out.println("output path: "+OUTPUT_PATH);
    if(cmd.hasOption(OPTION_SAMPLE_SIZE)){
      SAMPLE_SIZE = Integer.parseInt(cmd.getOptionValue(OPTION_SAMPLE_SIZE));
    }

  }



}
