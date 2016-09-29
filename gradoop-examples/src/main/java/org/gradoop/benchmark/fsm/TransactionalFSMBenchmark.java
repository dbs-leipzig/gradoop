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

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.fsm.common.config.CanonicalLabel;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.GrowthStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;
import org.gradoop.flink.algorithms.fsm.tfsm.TransactionalFSM;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized transactional FSM benchmark.
 */
public class TransactionalFSMBenchmark extends AbstractRunner
  implements ProgramDescription {

  /**
   * Option to declare path to input graph
   */
  private static final String INPUT_PATH = "ip";

  /**
   * Path to CSV log file
   */
  private static final String LOG_PATH = "lp";

  /**
   * Minimum support threshold
   */
  private static final String THRESHOLD = "ms";

  /**
   * directed flag
   */
  private static final String DIRECTED = "di";

  /**
   * flag for preprocessing
   */
  private static final String PREPROCESSING = "pp";

  // CANONICAL LABEL

  /**
   * Option to specify canonical label
   */
  private static final String CANONICAL = "cn";

  /**
   * Value enum map for iterations
   */
  private static final Map<String, CanonicalLabel> CANONICAL_DICT =
    Maps.newHashMap();

  /**
   * Set values
   */
  static {
    CANONICAL_DICT.put("cam", CanonicalLabel.ADJACENCY_MATRIX);
    CANONICAL_DICT.put("dfs", CanonicalLabel.MIN_DFS);
  }

  /**
   * implementation parameter values
   */
  private static final Collection<String> CANONICAL_VALUES =
    CANONICAL_DICT.keySet();

  // GROWTH

  /**
   * Option to set the growth strategy for children of frequent subgraphs.
   */
  private static final String GROWTH = "gr";

  /**
   * Value enum map for iterations
   */
  private static final Map<String, GrowthStrategy> GROWTH_DICT =
    Maps.newHashMap();

  /**
   * Set values
   */
  static {
    GROWTH_DICT.put("join", GrowthStrategy.JOIN);
    GROWTH_DICT.put("fusion", GrowthStrategy.FUSION);
  }

  /**
   * growth parameter values
   */
  private static final Collection<String> GROWTH_VALUES =
    GROWTH_DICT.keySet();

  // ITERATION

  /**
   * Option to set the iteration strategy
   */
  private static final String ITERATION = "it";

  /**
   * Value enum map for iterations
   */
  private static final Map<String, IterationStrategy> ITERATION_DICT =
    Maps.newHashMap();

  /**
   * Set values
   */
  static {
    ITERATION_DICT.put("bulk", IterationStrategy.BULK_ITERATION);
    ITERATION_DICT.put("unroll", IterationStrategy.LOOP_UNROLLING);
  }

  /**
   * implementation parameter values
   */
  private static final Collection<String> ITERATION_VALUES =
    ITERATION_DICT.keySet();

  static {
    OPTIONS.addOption(INPUT_PATH,
      "input-path", true, "path of graph files (hdfs)");
    OPTIONS.addOption(LOG_PATH,
      "log path", true, "path of the generated log file");
    OPTIONS.addOption(THRESHOLD,
      "minimum-support", true, "minimum support threshold");
    OPTIONS.addOption(DIRECTED,
      "directed", false, "Flag for directed graphs");
    OPTIONS.addOption(PREPROCESSING,
      "preprocessing", false, "Flag to enable preprocessing");
    OPTIONS.addOption(CANONICAL,
      "canonical label", true, "specify canonical label");
    OPTIONS.addOption(GROWTH,
      "growth strategy", true, "specify how to grow children");
    OPTIONS.addOption(ITERATION,
      "implementation", true, "gSpan implementation");
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
    String inputPath = cmd.getOptionValue(INPUT_PATH);
    String logPath = cmd.getOptionValue(LOG_PATH);

    float threshold = Float.parseFloat(cmd.getOptionValue(THRESHOLD));
    boolean directed = cmd.hasOption(DIRECTED);
    boolean usePreprocessing = cmd.hasOption(PREPROCESSING);
    String canonicalLabel = cmd.getOptionValue(CANONICAL);
    String growthStrategy = cmd.getOptionValue(GROWTH);
    String iterationStrategy = cmd.getOptionValue(ITERATION);

    // create gradoop conf
    GradoopFlinkConfig gradoopConfig = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    // read tlf graph
    TLFDataSource tlfSource = new TLFDataSource(inputPath, gradoopConfig);

    // create input dataset
    GraphTransactions graphs = tlfSource.getGraphTransactions();

    // set config for synthetic or real world dataset
    FSMConfig fsmConfig = new FSMConfig(
      threshold,
      directed,
      1,
      14,
      usePreprocessing,
      CANONICAL_DICT.get(canonicalLabel),
      FilterStrategy.BROADCAST_JOIN,
      GROWTH_DICT.get(growthStrategy),
      ITERATION_DICT.get(iterationStrategy)
    );

    // mine
    GraphTransactions frequentSubgraphs =
      new TransactionalFSM(fsmConfig).execute(graphs);

    // write statistics
    writeCSV(inputPath, directed, iterationStrategy , threshold, logPath,
      frequentSubgraphs.getTransactions().count());
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(INPUT_PATH)) {
      throw new IllegalArgumentException("Input path must not be empty.");
    }
    if (!cmd.hasOption(LOG_PATH)) {
      throw new IllegalArgumentException("Log file path must not be empty.");
    }
    if (!cmd.hasOption(THRESHOLD)) {
      throw new IllegalArgumentException("Minimum support must be specified.");
    }
    if (!cmd.hasOption(CANONICAL) ||
      !CANONICAL_VALUES.contains(cmd.getOptionValue(CANONICAL))) {
      throw new IllegalArgumentException(
        CANONICAL + "Must be in " + CANONICAL_VALUES);
    }
    if (!cmd.hasOption(GROWTH) ||
      !GROWTH_VALUES.contains(cmd.getOptionValue(GROWTH))) {
      throw new IllegalArgumentException(
        GROWTH + "Must be in " + GROWTH_VALUES);
    }
    if (!cmd.hasOption(ITERATION) ||
      !ITERATION_VALUES.contains(cmd.getOptionValue(ITERATION))) {
      throw new IllegalArgumentException(
        ITERATION + "Must be in " + ITERATION_VALUES);
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
