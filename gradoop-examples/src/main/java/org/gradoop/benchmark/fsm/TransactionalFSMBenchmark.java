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

package org.gradoop.benchmark.fsm;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.GSpanTLFGraphEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized transactional FSM benchmark.
 */
public class TransactionalFSMBenchmark extends AbstractRunner
  implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Path to CSV log file
   */
  private static final String OPTION_LOG_PATH = "log";

  /**
   * directed flag
   */
  private static final String OPTION_DIRECTED = "d";

  /**
   * implementation parameter
   */
  private static final String OPTION_IMPLEMENTATION = "impl";

  /**
   * implementation parameter values
   */
  private static final Collection<String> VALUES_IMPLEMENTATION =
    Lists.newArrayList(
      "it", // iterative
      "fr" // filter refinement
    );

  /**
   * Minimum support threshold
   */
  private static final String OPTION_THRESHOLD = "t";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH,
      "input-path", true, "path of graph files (hdfs)");
    OPTIONS.addOption(OPTION_LOG_PATH,
      "log path", true, "path of the generated log file");
    OPTIONS.addOption(OPTION_DIRECTED,
      "directed", false, "Flag for directed graphs");
    OPTIONS.addOption(OPTION_IMPLEMENTATION,
      "implementation", true, "gSpan implementation");
    OPTIONS.addOption(OPTION_THRESHOLD,
      "minimum-support", true, "minimum support threshold");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args,
      TransactionalFSMBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    // read cmd arguments
    String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    String logPath = cmd.getOptionValue(OPTION_LOG_PATH);
    float threshold = Float.parseFloat(cmd.getOptionValue(OPTION_THRESHOLD));
    boolean directed = cmd.hasOption(OPTION_DIRECTED);
    String implementation = cmd.getOptionValue(OPTION_IMPLEMENTATION);

    // create gradoop conf
    GradoopFlinkConfig gradoopConfig = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    // read tlf graph
    TLFDataSource tlfSource = new TLFDataSource(inputPath, gradoopConfig);

    // create input dataset
    DataSet<TLFGraph> graphs = tlfSource.getTLFGraphs();

    // set config for synthetic or real world dataset
    FSMConfig fsmConfig = new FSMConfig(threshold, directed);

    // set encoder
    GSpanEncoder encoder = new GSpanTLFGraphEncoder(fsmConfig);

    // set miner
    GSpanMiner miner;

    miner = implementation.equals("it") ?
      new GSpanBulkIteration() :
      new GSpanFilterRefine();

    miner.setExecutionEnvironment(getExecutionEnvironment());

    // encode
    DataSet<GSpanGraph> gsGraph = encoder.encode(graphs, fsmConfig);

    // mine
    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
      miner.mine(gsGraph, encoder.getMinFrequency(), fsmConfig);

    // write statistics
    writeCSV(inputPath, directed, implementation , threshold, logPath,
      frequentSubgraphs.count());
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Input path must not be empty.");
    }
    if (!cmd.hasOption(OPTION_LOG_PATH)) {
      throw new IllegalArgumentException("Log file path must not be empty.");
    }
    if (!cmd.hasOption(OPTION_THRESHOLD)) {
      throw new IllegalArgumentException("Minimum support must be specified.");
    }
    if (cmd.hasOption(OPTION_IMPLEMENTATION) && !VALUES_IMPLEMENTATION
      .contains(cmd.getOptionValue(OPTION_IMPLEMENTATION))) {
      throw new IllegalArgumentException("Invalid Implementation specified. " +
        "Must be on of " + VALUES_IMPLEMENTATION);
    }
  }

  /**
   * Method to create and add lines to a csv-file
   * @throws IOException
   * @param inputPath input file path
   * @param directed true, if directed graph
   * @param implementation gSpan implementation
   * @param threshold minimum support
   * @param logPath log path
   * @param subgraphCount subgraph count
   */
  private static void writeCSV(String inputPath, boolean directed,
    String implementation, float threshold, String logPath, long subgraphCount
  ) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s|%s%n",
      "Parallelism",
      "Implementation",
      "Dataset",
      "Directed",
      "Threshold",
      "Subgraphs",
      "Runtime"
    );

    String tail = String.format("%s|%s|%s|%s|%s|%s|%s%n",
      getExecutionEnvironment().getParallelism(),
      implementation,
      StringUtils.substringAfterLast(inputPath, "/"),
      directed,
      threshold,
      subgraphCount,
      getExecutionEnvironment()
        .getLastJobExecutionResult()
        .getNetRuntime(TimeUnit.SECONDS)
    );

    File f = new File(logPath);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(logPath, "UTF-8");
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
    return TransactionalFSMBenchmark.class.getName();
  }
}
