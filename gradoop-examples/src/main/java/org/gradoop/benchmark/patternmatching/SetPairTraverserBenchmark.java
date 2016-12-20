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
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairBulkTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.DistributedTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairForLoopTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.SetPairTraverser;

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
public class SetPairTraverserBenchmark extends TraverserBenchmark {
  /**
   * Constructor
   *
   * @param cmd command line
   */
  private SetPairTraverserBenchmark(CommandLine cmd) {
    super(cmd);
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
    CommandLine cmd = parseArguments(args, SetPairTraverserBenchmark.class.getName());
    if (cmd == null) {
      System.exit(1);
    }

    TraverserBenchmark benchmark = new SetPairTraverserBenchmark(cmd);

    benchmark.run();
    benchmark.close();
  }

  @Override
  void run() throws Exception {
    // read graph
    DataSet<IdWithCandidates<Long>> vertices = getExecutionEnvironment()
      .readCsvFile(getInputPath() + "vertices/")
      .types(Long.class)
      .map(new GetIdWithCandidates(getVertexCount()));

    DataSet<TripleWithCandidates<Long>> edges = getExecutionEnvironment()
      .readCsvFile(getInputPath() + "edges/")
      .fieldDelimiter(",")
      .types(Long.class, Long.class, Long.class)
      .map(new GetTriplesWithCandidates(getEdgeCount()));

    // create distributed traverser
    SetPairTraverser<Long> distributedTraverser;
    if (getTraverserStrategy() == TraverserStrategy.SET_PAIR_BULK_ITERATION) {
      distributedTraverser = new SetPairBulkTraverser<>(getTraversalCode(),
        getVertexCount(), getEdgeCount(), Long.class);
    } else {
      distributedTraverser = new SetPairForLoopTraverser<>(getTraversalCode(),
        getVertexCount(), getEdgeCount(), Long.class);
    }

    setEmbeddingCount(distributedTraverser.traverse(vertices, edges).count());
  }
}
