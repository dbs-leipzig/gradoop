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
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A program to create synthetic datasets of directed multigraphs.
 */
public class SyntheticDataGenerator
  extends AbstractRunner implements ProgramDescription {

  /**
   * Option to set path to output file
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to set graph count
   */
  public static final String OPTION_GRAPH_COUNT = "c";

  /**
   * Gradoop configuration
   */
  private static GradoopFlinkConfig GRADOOP_CONFIG =
    GradoopFlinkConfig.createConfig(getExecutionEnvironment());

  static {
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true, "Path to output file");
    OPTIONS.addOption(OPTION_GRAPH_COUNT, "graph-count", true, "Number of graphs");
  }

  /**
   * Main program to run the example. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SyntheticDataGenerator.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments from command line
    final String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    int graphCount = Integer.parseInt(cmd.getOptionValue(OPTION_GRAPH_COUNT));

    // Create data source and sink
    PredictableTransactionsGenerator dataSource =
      new PredictableTransactionsGenerator(graphCount, 1, true, GRADOOP_CONFIG);
    DataSink dataSink = new TLFDataSink(outputPath, GRADOOP_CONFIG);

    // execute and write to disk
    dataSink.write(
      GRADOOP_CONFIG.getGraphCollectionFactory().fromTransactions(dataSource.execute()),
      true);
    getExecutionEnvironment().execute();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("No output file specified.");
    }
    if (!cmd.hasOption(OPTION_GRAPH_COUNT)) {
      throw new IllegalArgumentException("No graph count specified.");
    } else {
      if (Integer.parseInt(cmd.getOptionValue(OPTION_GRAPH_COUNT)) % 10 != 0) {
        throw new IllegalArgumentException("Graph count must be dividable by 10");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return SyntheticDataGenerator.class.getName();
  }
}
