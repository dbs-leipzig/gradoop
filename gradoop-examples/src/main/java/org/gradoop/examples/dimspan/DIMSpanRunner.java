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
package org.gradoop.examples.dimspan;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.examples.dimspan.data_source.DIMSpanTLFSource;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A program to run DIMSpan standalone.
 */
public class DIMSpanRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Option to set path to input file
   */
  public static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to set path to output file
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to set minimum support threshold
   */
  public static final String OPTION_MIN_SUPPORT = "ms";

  /**
   * Option to enable undirected mining mode
   */
  public static final String OPTION_UNDIRECTED_MODE = "u";

  /**
   * Gradoop configuration
   */
  private static GradoopFlinkConfig GRADOOP_CONFIG =
    GradoopFlinkConfig.createConfig(getExecutionEnvironment());

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true, "Path to input file");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true, "Path to output file");
    OPTIONS.addOption(OPTION_MIN_SUPPORT, "min-support", true, "Minimum support threshold");
    OPTIONS.addOption(OPTION_UNDIRECTED_MODE, "undirected", false, "Enable undirected mode");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, DIMSpanRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);

    float minSupport = Float.valueOf(cmd.getOptionValue(OPTION_MIN_SUPPORT));

    boolean directed = !cmd.hasOption(OPTION_UNDIRECTED_MODE);

    // Create data source and sink
    DIMSpanTLFSource dataSource = new DIMSpanTLFSource(inputPath, GRADOOP_CONFIG);
    DataSink dataSink = new TLFDataSink(outputPath, GRADOOP_CONFIG);

    DIMSpanConfig fsmConfig = new DIMSpanConfig(minSupport, directed);

    // Change default configuration here using setter methods

    DataSet<GraphTransaction> frequentPatterns =
      new DIMSpan(fsmConfig).execute(dataSource.getGraphs());

    // Execute and write to disk
    dataSink.write(
      GRADOOP_CONFIG.getGraphCollectionFactory().fromTransactions(frequentPatterns),
      true);
    getExecutionEnvironment().execute();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("No input file specified.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("No output file specified.");
    }
    if (!cmd.hasOption(OPTION_MIN_SUPPORT)) {
      throw new IllegalArgumentException("No min support specified.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return DIMSpanRunner.class.getName();
  }
}
