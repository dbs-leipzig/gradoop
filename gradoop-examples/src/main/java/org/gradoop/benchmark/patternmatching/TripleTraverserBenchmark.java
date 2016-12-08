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

package org.gradoop.benchmark.patternmatching;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.DistributedTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TripleForLoopTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TripleTraverser;

import java.util.concurrent.TimeUnit;

/**
 * Used to benchmark different {@link DistributedTraverser} implementations.
 *
 * The benchmarks expects the graph to be stored in two different directories:
 *
 * [inputDir]/vertices -> [unique vertex-id]
 * [inputDir]/edges    -> [unique edge-id,source-vertex-id,target-vertex-id]
 *
 * All identifiers must be of type {@link Long}.
 */
public class TripleTraverserBenchmark extends TraverserBenchmark {
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_QUERY = "q";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph directory");
    OPTIONS.addOption(OPTION_QUERY, "query", true, "Pattern or fixed query");
  }

  /**
   * Constructor
   *
   * @param inputPath path to input graph data
   *
   */
  private TripleTraverserBenchmark(String inputPath) {
    super(inputPath);
  }

  /**
   * Given a query pattern, the benchmark computes the number of matching
   * subgraphs in the given data graph.
   *
   * This benchmark currently supports structure only pattern. For semantic
   * patterns use {@link PatternMatching}.
   *
   * usage: org.gradoop.benchmark.patternmatching.TraverserBenchmark
   * [-i <arg>] [-q <arg>] [-t <arg>]
   * -i,--input <arg>       Graph directory
   * -q,--query <arg>       Pattern or fixed query (e.g. q2 or "(a)-->(b)")
   * -t,--traverser <arg>   [loop|bulk]
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TripleTraverserBenchmark.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    TraverserBenchmark benchmark = new TripleTraverserBenchmark(
      cmd.getOptionValue(OPTION_INPUT_PATH));

    benchmark.initialize(cmd.getOptionValue(OPTION_QUERY));
    benchmark.run();
  }

  @Override
  public void run() throws Exception {
    // read graph
    DataSet<TripleWithCandidates<Long>> triples = getExecutionEnvironment()
      .readCsvFile(getInputPath())
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class)
      .map(new GetTriplesWithCandidates(getEdgeCount()));

    // create distributed traverser
    TripleTraverser<Long> distributedTraverser =
      new TripleForLoopTraverser<>(getTraversalCode(), getVertexCount(), getEdgeCount(), Long.class);

    // print embedding count
    long embeddingCount = distributedTraverser.traverse(triples).count();

    System.out.println("embeddingCount = " + embeddingCount);

    long duration = getExecutionEnvironment().getLastJobExecutionResult().getNetRuntime();

    System.out.println("duration = " + duration);
  }
}
