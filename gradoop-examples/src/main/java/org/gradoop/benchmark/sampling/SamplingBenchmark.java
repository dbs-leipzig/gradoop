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
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program to evaluate sampling algorithms on the basis of a social network graphs.
 */
public class SamplingBenchmark extends AbstractRunner implements ProgramDescription {

  /**
   * Required option to declare the path to the directory containing csv files to be processed.
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare the format of the input data.
   */
  private static final String OPTION_INPUT_FORMAT = "f";
  /**
   * Option to declare the path to output directory.
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to define the to-be-evaluated sampling algorithm.
   *
   * Available mappings:
   * 0 ---> PageRankSampling
   * 1 ---> RandomEdgeSampling
   * 2 ---> RandomLimitedDegreeVertexSampling
   * 3 ---> RandomNonUniformVertexSampling
   * 4 ---> RandomVertexEdgeSampling
   * 5 ---> RandomVertexNeighborhoodSampling
   * 6 ---> RandomVertexSampling
   */
  private static final String OPTION_SELECTED_ALGORITHM = "a";
  /**
   * Option to declare list of parameters that are passed on to a constructor.
   */
  private static final String OPTION_CONSTRUCTOR_PARAMS = "p";
  /**
   * Used input path.
   */
  private static String INPUT_PATH;
  /**
   * Used output path.
   */
  private static String OUTPUT_PATH;
  /**
   * Format of used input data.
   */
  private static String INPUT_FORMAT;
  /**
   * Default format of input data.
   */
  private static final String INPUT_FORMAT_DEFAULT = "csv";
  /**
   * Output path default.
   */
  private static final String OUTPUT_PATH_DEFAULT = "./sampling_benchmark/";
  /**
   * Output path suffix defining where resulting graph sample is written to.
   */
  private static final String OUTPUT_PATH_GRAPH_SAMPLE_SUFFIX = "graph_sample/";
  /**
   * Output path suffix defining where resulting benchmark file is written to.
   */
  private static final String OUTPUT_PATH_BENCHMARK_SUFFIX = "benchmark";
  /**
   * Integer defining the sampling algorithm that is to be evaluated.
   */
  private static int SELECTED_ALGORITHM;
  /**
   * List of parameters that are used to instantiate the selected sampling algorithm.
   */
  private static String[] CONSTRUCTOR_PARAMS;


  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to directory containing csv files to be processed");
    OPTIONS.addRequiredOption(OPTION_SELECTED_ALGORITHM, "algorithm", true,
      "Positive integer selecting a sampling algorithm");
    OPTIONS.addRequiredOption(OPTION_CONSTRUCTOR_PARAMS, "params", true,
      "Whitespace separated list of algorithm parameters");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to directory where resulting graph sample, benchmark file and graph " +
        "statistics are written to. (Defaults to " + OUTPUT_PATH_DEFAULT + ")");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Format of the input data. Defaults to 'csv'");
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

    LogicalGraph graph = readLogicalGraph(INPUT_PATH, INPUT_FORMAT);

    // instantiate selected sampling algorithm and create sample
    SamplingAlgorithm algorithm = SamplingBuilder.buildSelectedSamplingAlgorithm(
      SELECTED_ALGORITHM, CONSTRUCTOR_PARAMS);
    LogicalGraph graphSample = algorithm.execute(graph);

    // write graph sample and benchmark data
    writeLogicalGraph(graphSample, OUTPUT_PATH + OUTPUT_PATH_GRAPH_SAMPLE_SUFFIX);
    writeBenchmark(graphSample.getConfig().getExecutionEnvironment(), algorithm);
  }

  /**
   * Reads the given arguments from command line.
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH          = cmd.getOptionValue(OPTION_INPUT_PATH);
    SELECTED_ALGORITHM  = Integer.parseInt(cmd.getOptionValue(OPTION_SELECTED_ALGORITHM));
    CONSTRUCTOR_PARAMS  = cmd.getOptionValues(OPTION_CONSTRUCTOR_PARAMS);
    OUTPUT_PATH         = cmd.getOptionValue(OPTION_OUTPUT_PATH, OUTPUT_PATH_DEFAULT);
    INPUT_FORMAT        = cmd.getOptionValue(OPTION_INPUT_FORMAT, INPUT_FORMAT_DEFAULT);
  }

  /**
   * Method to crate and add lines to a benchmark file.
   *
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

  @Override
  public String getDescription() {
    return SamplingBenchmark.class.getName();
  }
}
