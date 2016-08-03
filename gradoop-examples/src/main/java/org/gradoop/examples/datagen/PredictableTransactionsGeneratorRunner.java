/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.datagen;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.tlf.TLFDataSink;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Class to run the PredictableTransactionGenerator with console parameters
 */
public class PredictableTransactionsGeneratorRunner extends AbstractRunner
  implements ProgramDescription {
  /**
   * Option to declare path to output graph
   */
  public static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Graph count option for PTGenerator
   */
  public static final String OPTION_GRAPH_COUNT = "gc";
  /**
   * Graph size option for PTGenerator
   */
  public static final String OPTION_GRAPH_SIZE = "gs";
  /**
   * Multi graph option for PTGenerator
   */
  public static final String OPTION_MULTI_GRAPH = "mg";


  static {
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Output path for generator.");
    OPTIONS.addOption(OPTION_GRAPH_COUNT, "graph-count", true, "Graph count " +
      "option for generator");
    OPTIONS.addOption(OPTION_GRAPH_SIZE, "graph-size", true, "Graph size " +
      "option for generator");
    OPTIONS.addOption(OPTION_MULTI_GRAPH, "multi-graph", false, "Multi graph " +
      "option for generator");
  }
  /**
   * Main program to run the generator. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args,
      PredictableTransactionsGeneratorRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read arguments
    String outputPath = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    long graphCount = Long.parseLong(cmd.getOptionValue(OPTION_GRAPH_COUNT));
    int graphSize = Integer.parseInt(cmd.getOptionValue(OPTION_GRAPH_SIZE));
    boolean multiGraph = cmd.hasOption(OPTION_MULTI_GRAPH);

    // initialize generator
    PredictableTransactionsGenerator dataGen = new
      PredictableTransactionsGenerator(graphCount, graphSize, multiGraph,
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    // execute generator
    GraphTransactions generatedGraph = dataGen.execute();

    // build result name
    String fileName = "predictable_" + graphCount + "_" + graphSize;

    if (!multiGraph) {
      fileName += "_simple";
    }

    fileName += ".tlf";

    // write output
    DataSink dataSink = new TLFDataSink(outputPath + fileName,
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    dataSink.write(generatedGraph);

    //execute
    getExecutionEnvironment().execute();
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph output directory.");
    }
    if (!cmd.hasOption(OPTION_GRAPH_COUNT)) {
      throw new IllegalArgumentException("Define graph count");
    }
    if (!cmd.hasOption(OPTION_GRAPH_SIZE)) {
      throw new IllegalArgumentException("Define graph size");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return PredictableTransactionsGeneratorRunner.class.getName();
  }
}
