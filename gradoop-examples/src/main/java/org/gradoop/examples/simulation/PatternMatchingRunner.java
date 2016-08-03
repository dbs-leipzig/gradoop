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

package org.gradoop.examples.simulation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.DualSimulation;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST;

/**
 * Performs graph pattern matching on an arbitrary input graph.
 */
public class PatternMatchingRunner extends AbstractRunner implements
  ProgramDescription {
  /**
   * Refers to {@link DualSimulation} using bulk iteration
   */
  private static final String ALGO_DUAL_BULK = "dual-bulk";
  /**
   * Refers to {@link DualSimulation} using delta iteration
   */
  private static final String ALGO_DUAL_DELTA = "dual-delta";
  /**
   * Refers to {@link ExplorativeSubgraphIsomorphism}
   */
  private static final String ALGO_ISO_EXP = "iso-exp";
  /**
   * Refers to {@link ExplorativeSubgraphIsomorphism} using
   * BROADCAST_HASH_FIRST as {@link JoinHint} for extending embeddings.
   */
  private static final String ALGO_ISO_EXP_BC_HASH_FIRST = "iso-exp-bc-hf";
  /**
   * Used for console output
   */
  private static final String[] AVAILABLE_ALGORITHMS = new String[] {
      ALGO_DUAL_BULK,
      ALGO_DUAL_DELTA,
      ALGO_ISO_EXP,
      ALGO_ISO_EXP_BC_HASH_FIRST
  };
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH  = "i";
  /**
   *Option to declare path to output graph
    */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * GDL query string
   */
  private static final String OPTION_QUERY_GRAPH = "q";
  /**
   * Pattern Matching algorithm
   */
  private static final String OPTION_ALGORITHM = "a";
  /**
   * Attach original data true/false
   */
  private static final String OPTION_ATTACH_DATA = "d";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true,
      "Input graph directory (EPGM json format)");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Output graph directory");
    OPTIONS.addOption(OPTION_QUERY_GRAPH, "query", true,
      "GDL based query graph");
    OPTIONS.addOption(OPTION_ALGORITHM, "algorithm", true,
      String.format("Algorithm to execute the matching [%s]",
        StringUtils.join(AVAILABLE_ALGORITHMS, ',')));
    OPTIONS.addOption(OPTION_ATTACH_DATA, "attach-data", false,
      "Attach original vertex and edge data to the match graph");
  }

  /**
   * Runs the simulation.
   *
   * @param args args[0]: input dir, args[1]: output dir
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args,
      PatternMatchingRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    String inputDir     = cmd.getOptionValue(OPTION_INPUT_PATH);
    String outputDir    = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    String query        = cmd.getOptionValue(OPTION_QUERY_GRAPH);
    String algorithm    = cmd.getOptionValue(OPTION_ALGORITHM);
    boolean attachData  = cmd.hasOption(OPTION_ATTACH_DATA);

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);

    GraphCollection result = execute(
      epgmDatabase, query, attachData, algorithm);

    writeGraphCollection(result, outputDir);

    System.out.println(String.format("Net runtime [s]: %d",
      getExecutionEnvironment()
        .getLastJobExecutionResult()
        .getNetRuntime(TimeUnit.SECONDS)));
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(final CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define an graph output directory.");
    }
    if (!cmd.hasOption(OPTION_QUERY_GRAPH)) {
      throw new IllegalArgumentException("Define a graph query.");
    }
    if (!cmd.hasOption(OPTION_ALGORITHM)) {
      throw new IllegalArgumentException("Chose an algorithm.");
    }
  }

  /**
   * Executes dual simulation on the given logical graph.
   *
   * @param databaseGraph data graph
   * @param query         query graph
   * @param attachData    attach vertex and edge data to the match graph
   * @param algorithm     algorithm to use for pattern matching
   * @return result match graph
   */
  private static GraphCollection execute(
    LogicalGraph databaseGraph,
    String query, boolean attachData, String algorithm) {

    PatternMatching op;

    switch (algorithm) {
    case ALGO_DUAL_BULK:
      op = new DualSimulation(query, attachData, true);
      break;
    case ALGO_DUAL_DELTA:
      op = new DualSimulation(query, attachData, false);
      break;
    case ALGO_ISO_EXP:
      op = new ExplorativeSubgraphIsomorphism(query, attachData);
      break;
    case ALGO_ISO_EXP_BC_HASH_FIRST:
      op = new ExplorativeSubgraphIsomorphism(query, attachData,
        new DFSTraverser(), BROADCAST_HASH_FIRST, BROADCAST_HASH_FIRST);
      break;
    default :
      throw new IllegalArgumentException(algorithm + " not supported");
    }

    return op.execute(databaseGraph);
  }

  @Override
  public String getDescription() {
    return PatternMatchingRunner.class.getName();
  }
}
