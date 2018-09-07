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
package org.gradoop.benchmark.sampling;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexEdgeSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomNonUniformVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling;
import org.gradoop.flink.model.impl.operators.sampling.PageRankSampling;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.utils.statistics.StatisticsRunner;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program to evaluate sampling algorithms on the basis of ldbc generated social network
 * graphs
 */
public class SamplingBenchmark extends AbstractRunner implements ProgramDescription {

  /**
   * Required option to declare the path to the directory containing csv files to be processed
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare the path to output directory
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  /**
   * Option to define to-be-evaluated sampling algorithm
   */
  private static final String OPTION_SELECTED_ALGORITHM = "a";

  /**
   * Option to declare list of parameters that are passed on to a constructor
   */
  private static final String OPTION_CONSTRUCTOR_PARAMS = "p";

  /**
   * Option to to set whether graph statistics are to be computed
   */
  private static final String OPTION_COMPUTE_STATISTICS = "s";

  /**
   * Used input path
   */
  private static String INPUT_PATH;

  /**
   * Used output path
   */
  private static String OUTPUT_PATH;

  /**
   * Output path default
   */
  private static final String OUTPUT_PATH_DEFAULT = "./sampling_benchmark/";

  /**
   * Output path suffix defining where resulting graph sample is written to
   */
  private static final String OUTPUT_PATH_GRAPH_SAMPLE_SUFFIX = "graph_sample/";

  /**
   * Output path suffix defining where resulting benchmark file is written to
   */
  private static final String OUTPUT_PATH_BENCHMARK_SUFFIX = "benchmark";

  /**
   * Output path suffix defining where input graph statistics are written to
   */
  private static final String GRAPH_STATISTICS_ORIGIN = "graph_statistics/origin/";

  /**
   * Output path suffix defining where sample graph statistics are written to
   */
  private static final String GRAPH_STATISTICS_SAMPLE = "graph_statistics/sample/";

  /**
   * Integer defining the sampling algorithm that is to be evaluated.
   */
  private static int SELECTED_ALGORITHM;

  /**
   * List of parameters that are used to instantiate the selected sampling algorithm.
   */
  private static String[] CONSTRUCTOR_PARAMS;

  /**
   * Boolean determining if graph statistics for input and sample graph shall be computed
   */
  private static boolean COMPUTE_STATISTICS;

  /**
   * Enum of available sampling algorithms. Enum is constructed with meta information about a given
   * sampling algorithm like its name.
   */
  private enum Algorithm {
    /** PageRankSampling enum constant */
    PAGE_RANK_SAMPLING(PageRankSampling.class.getSimpleName()),
    /** RandomEdgeSampling enum constant */
    RANDOM_EDGE_SAMPLING(RandomEdgeSampling.class.getSimpleName()),
    /** RandomLimitedDegreeVertexSampling enum constant */
    RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING(RandomLimitedDegreeVertexSampling.class.getSimpleName()),
    /** RandomNonUniformVertexSampling enum constant */
    RANDOM_NON_UNIFORM_VERTEX_SAMPLING(RandomNonUniformVertexSampling.class.getSimpleName()),
    /** RandomVertexEdgeSampling enum constant */
    RANDOM_VERTEX_EDGE_SAMPLING(RandomVertexEdgeSampling.class.getSimpleName()),
    /** RandomVertexNeighborhoodSampling enum constant */
    RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING(RandomVertexNeighborhoodSampling.class.getSimpleName()),
    /** RandomVertexSampling enum constant */
    RANDOM_VERTEX_SAMPLING(RandomVertexSampling.class.getSimpleName());

    /** Property denoting the simple classname of a sampling algorithm */
    private final String name;

    /**
     * Enum constructor
     * @param name simplified class name
     */
    Algorithm(String name) {
      this.name = name;
    }
  }

  static {
    OPTIONS.addOption(Option.builder(OPTION_INPUT_PATH)
      .longOpt("input")
      .hasArg()
      .desc("Path to directory containing csv files to be processed")
      .required()
      .build());
    OPTIONS.addOption(Option.builder(OPTION_SELECTED_ALGORITHM)
      .longOpt("algorithm")
      .hasArg()
      .desc("Positive integer selecting a sampling algorithm")
      .required()
      .build());
    OPTIONS.addOption(Option.builder(OPTION_CONSTRUCTOR_PARAMS)
      .longOpt("params")
      .hasArgs()
      .desc("Whitespace separated list of algorithm parameters")
      .required()
      .build());
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to directory where resulting graph sample, benchmark file and graph " +
        "statistics are written to. (Defaults to " + OUTPUT_PATH_DEFAULT + ")");
    OPTIONS.addOption(OPTION_COMPUTE_STATISTICS, false,
      "If provided, graph statistics for input and sample graph are computed.");
  }

  /**
   * Main program to run the benchmark. Required arguments are a path to CSVDataSource compatible
   * files that define a graph, an integer defining the sampling algorithm to be tested and a list
   * of parameters for the constructor of the sampling class.
   * Other arguments are the available options.
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SamplingBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    readCMDArguments(cmd);

    // set flink environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read logical graph
    DataSource source = new CSVDataSource(INPUT_PATH, conf);
    LogicalGraph graph = source.getLogicalGraph();

    // instantiate selected sampling algorithm and create sample
    SamplingAlgorithm algorithm = instantiateSelectedSamplingAlgorithm();
    LogicalGraph graphSample = algorithm.execute(graph);

    // write data to sink, overwrite if necessary
    DataSink sink = new CSVDataSink(OUTPUT_PATH + OUTPUT_PATH_GRAPH_SAMPLE_SUFFIX, conf);
    sink.write(graphSample, true);

    env.execute();

    writeBenchmark(env, algorithm);

    if (COMPUTE_STATISTICS) {
      // compute input graph statistics
      StatisticsRunner.main(
        new String[]{
            OUTPUT_PATH + OUTPUT_PATH_GRAPH_SAMPLE_SUFFIX,
            "csv",
            OUTPUT_PATH + GRAPH_STATISTICS_ORIGIN
        });
      // compute sample graph statistics
      StatisticsRunner.main(
        new String[]{INPUT_PATH, "csv", OUTPUT_PATH + GRAPH_STATISTICS_SAMPLE});
    }
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH          = cmd.getOptionValue(OPTION_INPUT_PATH);
    SELECTED_ALGORITHM  = Integer.parseInt(cmd.getOptionValue(OPTION_SELECTED_ALGORITHM));
    CONSTRUCTOR_PARAMS  = cmd.getOptionValues(OPTION_CONSTRUCTOR_PARAMS);
    OUTPUT_PATH         = cmd.getOptionValue(OPTION_OUTPUT_PATH, OUTPUT_PATH_DEFAULT);
    COMPUTE_STATISTICS  = cmd.hasOption(OPTION_COMPUTE_STATISTICS);
  }

  /**
   * Method to crate and add lines to a benchmark file
   * @param env given ExecutionEnvironment
   * @param sampling sampling algorithm under test
   * @throws IOException exception during file writing
   */
  private static void writeBenchmark(ExecutionEnvironment env, SamplingAlgorithm sampling)
      throws IOException {
    String head = String.format("%s|%s|%s|%s|%s%n",
      "Parallelism", "Dataset", "Algorithm", "Params", "Runtime [s]");

    // build log
    String samplingName = sampling.getClass().getSimpleName();
    String tail = String.format("%s|%s|%s|%s|%s%n",
      env.getParallelism(),
      INPUT_PATH.substring(INPUT_PATH.lastIndexOf(File.separator) + 1),
      samplingName,
      String.join(", ", CONSTRUCTOR_PARAMS),
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(OUTPUT_PATH + OUTPUT_PATH_BENCHMARK_SUFFIX);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(OUTPUT_PATH + OUTPUT_PATH_BENCHMARK_SUFFIX, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  /**
   * Tries to instantiate a SamplingAlgorithm defined by the enum Algorithm. Maps the
   * value of SELECTED_ALGORITHM and the length of CONSTRUCTOR_PARAMS to a specific constructor.
   * Constructors that take non-primitive data types as input are not provided.
   * If the value of SELECTED_ALGORITHM does not match any ordinal of the enum, or the length of
   * CONSTRUCTOR_PARAMS does not match any signature of a specified SamplingAlgorithm, an exception
   * is thrown.
   *
   * @return SamplingAlgorithm specified by const SELECTED_ALGORITHM
   */
  private static SamplingAlgorithm instantiateSelectedSamplingAlgorithm() {

    if (SELECTED_ALGORITHM < 0 || SELECTED_ALGORITHM > Algorithm.values().length - 1) {
      throw new IllegalArgumentException(String.format("No sampling algorithm associated with %d." +
          " Please provide an integer in the range 0 - %d%n%s",
        SELECTED_ALGORITHM,
        Algorithm.values().length - 1,
        getOrdinalAlgorithmAssociation()));
    }

    switch (Algorithm.values()[SELECTED_ALGORITHM]) {

    case PAGE_RANK_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 5) {
        return new PageRankSampling(
          Double.parseDouble(CONSTRUCTOR_PARAMS[0]),
          Integer.parseInt(CONSTRUCTOR_PARAMS[1]),
          Double.parseDouble(CONSTRUCTOR_PARAMS[2]),
          Boolean.parseBoolean(CONSTRUCTOR_PARAMS[3]),
          Boolean.parseBoolean(CONSTRUCTOR_PARAMS[4]));
      } else {
        throw createInstantiationException(Algorithm.PAGE_RANK_SAMPLING.name, new String[]{"5"});
      }

    case RANDOM_EDGE_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomEdgeSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomEdgeSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Long.parseLong(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_EDGE_SAMPLING.name,
          new String[]{"1", "2"});
      }

    case RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomLimitedDegreeVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomLimitedDegreeVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Long.parseLong(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING.name,
          new String[]{"1", "2"});
      }

    case RANDOM_NON_UNIFORM_VERTEX_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomNonUniformVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomNonUniformVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Long.parseLong(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_NON_UNIFORM_VERTEX_SAMPLING.name,
          new String[]{"1", "2"});
      }

    case RANDOM_VERTEX_EDGE_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomVertexEdgeSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomVertexEdgeSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Float.parseFloat(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_EDGE_SAMPLING.name,
          new String[]{"1", "2"});
      }

    case RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomVertexNeighborhoodSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomVertexNeighborhoodSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Long.parseLong(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING.name,
          new String[]{"1", "2"});
      }

    case RANDOM_VERTEX_SAMPLING:
      if (CONSTRUCTOR_PARAMS.length == 1) {
        return new RandomVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]));
      } else if (CONSTRUCTOR_PARAMS.length == 2) {
        return new RandomVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0]),
          Long.parseLong(CONSTRUCTOR_PARAMS[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_SAMPLING.name,
          new String[]{"1", "2"});
      }

    default:
      throw  new IllegalArgumentException(
        "Something went wrong. Please select an other sampling algorithm.");
    }
  }

  /**
   * Helper function to create string that can be used to display available associations between the
   * ordinals of the enum Algorithm and the sampling algorithms themselves.
   *
   * @return String providing association information.
   */
  private static String getOrdinalAlgorithmAssociation() {
    StringBuilder result = new StringBuilder("Available Algorithms:\n");
    for (Algorithm alg : Algorithm.values()) {
      result.append(String.format("%d ---> %s%n", alg.ordinal(), alg.name));
    }
    return result.toString();
  }

  /**
   * Helper function used to create an exception that is specific to function
   * instantiateSelectedSamplingAlgorithm()
   *
   * @param name Name of the sampling class.
   * @param amountArgs Array of strings containing all possible argument counts for a given sampling
   *                  class.
   * @return IllegalArgumentException
   */
  private static IllegalArgumentException createInstantiationException(
    String name,
    String[] amountArgs
  ) {
    return new IllegalArgumentException(String.format(
      "Constructor of %s requires %s arguments. %d provided.",
      name, String.join(" or ", amountArgs), CONSTRUCTOR_PARAMS.length
    ));
  }

  @Override
  public String getDescription() {
    return SamplingBenchmark.class.getName();
  }
}
