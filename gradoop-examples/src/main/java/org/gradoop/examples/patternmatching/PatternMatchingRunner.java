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
package org.gradoop.examples.patternmatching;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsHDFSReader;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.DualSimulation;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST;

/**
 * This program can be used to run the different pattern matching engines implemented in Gradoop.
 */
public class PatternMatchingRunner extends AbstractRunner implements ProgramDescription {
  /**
   * Refers to {@link DualSimulation} using bulk iteration
   */
  private static final String ALGO_DUAL_BULK = "dual-bulk";
  /**
   * Refers to {@link DualSimulation} using delta iteration
   */
  private static final String ALGO_DUAL_DELTA = "dual-delta";
  /**
   * Refers to {@link ExplorativePatternMatching}
   */
  private static final String ALGO_ISO_EXP = "iso-exp";
  /**
   * Refers to {@link ExplorativePatternMatching} using
   * BROADCAST_HASH_FIRST as {@link JoinHint} for extending embeddings.
   */
  private static final String ALGO_ISO_EXP_BC_HASH_FIRST = "iso-exp-bc-hf";
  /**
   * Refers to {@link CypherPatternMatching}.
   */
  private static final String ALGO_CYPHER = "cypher";
  /**
   * Used for console output
   */
  private static final String[] AVAILABLE_ALGORITHMS = new String[] {
      ALGO_DUAL_BULK,
      ALGO_DUAL_DELTA,
      ALGO_ISO_EXP,
      ALGO_ISO_EXP_BC_HASH_FIRST,
      ALGO_CYPHER
  };
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH  = "i";
  /**
   * Option to declare input graph format (csv, indexed, json)
   */
  private static final String OPTION_INPUT_FORMAT = "f";
  /**
   *Option to declare path to output graph
    */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * GDL/Cypher query string
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
  /**
   * Option to declare path to graph statistics
   */
  private static final String OPTION_STATISTICS = "s";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input-path", true,
      "Input graph directory");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "input-format", true,
      "Format of the input graph [csv, indexed, json]");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output-path", true,
      "Output graph directory");
    OPTIONS.addOption(OPTION_QUERY_GRAPH, "query", true,
      "GDL/Cypher based query graph");
    OPTIONS.addOption(OPTION_ALGORITHM, "algorithm", true,
      String.format("Algorithm to execute the matching [%s]",
        StringUtils.join(AVAILABLE_ALGORITHMS, ',')));
    OPTIONS.addOption(OPTION_ATTACH_DATA, "attach-data", false,
      "Attach original vertex and edge data to the match graph");
    OPTIONS.addOption(OPTION_STATISTICS, "statistics", true,
      "Path to graph statistics used for Cypher engine");
  }

  /**
   * Runs the simulation using the given arguments.
   * <p>
   * -i, --input-path (path to data graph)<br />
   * -f, --input-format (format of data graph [csv, indexed, json])<br />
   * -o, --output-path (path to output directory)<br />
   * -q, --query (GDL/Cypher query string)<br />
   * -a, --algorithm [dual-bulk,dual-delta,iso-exp,iso-exp-bc-hf,cypher]<br />
   * -d, --attach-data (default: false)<br />
   * -s, --statistics (path to input graph statistics, used for cypher engine)<br />
   *
   * @param args option line
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args,
      PatternMatchingRunner.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    String inputDir       = cmd.getOptionValue(OPTION_INPUT_PATH);
    String inputFormat    = cmd.getOptionValue(OPTION_INPUT_FORMAT).toLowerCase();
    String outputDir      = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    String query          = cmd.getOptionValue(OPTION_QUERY_GRAPH);
    String algorithm      = cmd.getOptionValue(OPTION_ALGORITHM);
    boolean attachData    = cmd.hasOption(OPTION_ATTACH_DATA);
    boolean hasStatistics = cmd.hasOption(OPTION_STATISTICS);

    GraphStatistics statistics = null;
    if (hasStatistics) {
      String statisticsPath = cmd.getOptionValue(OPTION_STATISTICS);
      if (statisticsPath.startsWith("hdfs://")) {
        statistics = GraphStatisticsHDFSReader.read(statisticsPath, new Configuration());
      } else {
        statistics = GraphStatisticsLocalFSReader.read(statisticsPath);
      }
    }

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir, inputFormat);

    GraphCollection result = execute(epgmDatabase, query, attachData, algorithm, statistics);

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
    if (!cmd.hasOption(OPTION_INPUT_FORMAT)) {
      throw new IllegalArgumentException("Define an input format");
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
    if (cmd.getOptionValue(OPTION_ALGORITHM).equals(ALGO_CYPHER)) {
      if (!cmd.hasOption(OPTION_STATISTICS)) {
        throw new IllegalArgumentException("Provide graph statistics when using Cypher engine");
      }
    }
  }

  /**
   * Executes dual simulation on the given logical graph.
   *
   * @param databaseGraph data graph
   * @param query         query graph
   * @param attachData    attach vertex and edge data to the match graph
   * @param algorithm     algorithm to use for pattern matching
   * @param statistics    statistics about the input graph (needed for cypher)
   * @return matching subgraphs
   */
  private static GraphCollection execute(
    LogicalGraph databaseGraph,
    String query, boolean attachData, String algorithm, GraphStatistics statistics) {

    PatternMatching op;

    switch (algorithm) {
    case ALGO_DUAL_BULK:
      op = new DualSimulation(query, attachData, true);
      break;
    case ALGO_DUAL_DELTA:
      op = new DualSimulation(query, attachData, false);
      break;
    case ALGO_ISO_EXP:
      op = new ExplorativePatternMatching.Builder()
        .setQuery(query)
        .setAttachData(attachData)
        .setMatchStrategy(MatchStrategy.ISOMORPHISM)
        .build();
      break;
    case ALGO_ISO_EXP_BC_HASH_FIRST:
      op = new ExplorativePatternMatching.Builder()
        .setQuery(query)
        .setAttachData(attachData)
        .setMatchStrategy(MatchStrategy.ISOMORPHISM)
        .setTraverserStrategy(TraverserStrategy.SET_PAIR_BULK_ITERATION)
        .setEdgeStepJoinStrategy(BROADCAST_HASH_FIRST)
        .setVertexStepJoinStrategy(BROADCAST_HASH_FIRST)
        .build();
      break;
    case ALGO_CYPHER:
      op = new CypherPatternMatching(query, attachData,
        MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
        statistics);
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
