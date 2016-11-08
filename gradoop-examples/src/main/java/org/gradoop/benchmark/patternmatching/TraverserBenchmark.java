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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.IterationStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.BulkTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.DistributedTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.ForLoopTraverser;

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
public class TraverserBenchmark extends AbstractRunner {
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_QUERY = "q";
  /**
   * Option to set the traverser
   */
  private static final String OPTION_TRAVERSER = "t";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph directory");
    OPTIONS.addOption(OPTION_QUERY, "query", true, "Pattern or fixed query");
    OPTIONS.addOption(OPTION_TRAVERSER, "traverser", true, "[loop|bulk]");
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
    CommandLine cmd = parseArguments(args, TraverserBenchmark.class.getName());
    if (cmd == null) {
      System.exit(1);
    }
    String inputPath   = cmd.getOptionValue(OPTION_INPUT_PATH);
    String queryString = cmd.getOptionValue(OPTION_QUERY);
    String itStrategy  = cmd.getOptionValue(OPTION_TRAVERSER);

    IterationStrategy iterationStrategy = (itStrategy.equals("bulk")) ?
      IterationStrategy.BULK_ITERATION : IterationStrategy.LOOP_UNROLLING;

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    int vertexCount;
    int edgeCount;
    TraversalCode tc;

    if (queryString.toLowerCase().startsWith("q")) {
      // fixed query
      Queries.Query query = getQuery(queryString.toLowerCase());
      vertexCount = query.getVertexCount();
      edgeCount = query.getEdgeCount();
      tc = query.getTraversalCode();
    } else {
      // GDL query pattern
      QueryHandler queryHandler = new QueryHandler(queryString);
      vertexCount = queryHandler.getVertexCount();
      edgeCount = queryHandler.getEdgeCount();
      Traverser traverser = new DFSTraverser();
      traverser.setQueryHandler(queryHandler);
      tc = traverser.traverse();
    }

    // read graph
    DataSet<IdWithCandidates<Long>> vertices = env
      .readCsvFile(inputPath + "vertices/")
      .types(Long.class)
      .map(new GetIdWithCandidates(vertexCount));

    DataSet<TripleWithCandidates<Long>> edges = env
      .readCsvFile(inputPath + "edges/")
      .fieldDelimiter(",")
      .types(Long.class, Long.class, Long.class)
      .map(new GetTriplesWithCandidates(edgeCount));

    // create distributed traverser
    DistributedTraverser<Long> distributedTraverser;
    if (iterationStrategy == IterationStrategy.BULK_ITERATION) {
      distributedTraverser = new BulkTraverser<>(tc,
        vertexCount, edgeCount, Long.class);
    } else {
      distributedTraverser = new ForLoopTraverser<>(tc,
        vertexCount, edgeCount, Long.class);
    }

    // print embedding count
    long embeddingCount = distributedTraverser
      .traverse(vertices, edges).count();

    System.out.println("embeddingCount = " + embeddingCount);

    long duration = env.getLastJobExecutionResult()
      .getNetRuntime(TimeUnit.SECONDS);

    System.out.println("duration = " + duration);
  }

  /**
   * Returns the query based on the input string
   *
   * @param queryString query identifier (q1, q2, ..., q7)
   * @return query
   */
  private static Queries.Query getQuery(String queryString) {
    Queries.Query query;
    switch (queryString) {
    case "q1":
      query = Queries.q1();
      break;
    case "q2":
      query = Queries.q2();
      break;
    case "q3":
      query = Queries.q3();
      break;
    case "q4":
      query = Queries.q4();
      break;
    case "q5":
      query = Queries.q5();
      break;
    case "q6":
      query = Queries.q6();
      break;
    case "q7":
      query = Queries.q7();
      break;
    default:
      throw new IllegalArgumentException("unsupported query: " + queryString);
    }
    return query;
  }

  /**
   * Initializes {@link IdWithCandidates}
   */
  public static class GetIdWithCandidates
    implements MapFunction<Tuple1<Long>, IdWithCandidates<Long>> {
    /**
     * Reduce object instantiations
     */
    private final IdWithCandidates<Long> reuseTuple;

    /**
     * Constructor
     *
     * @param vertexCount number of query vertices
     */
    public GetIdWithCandidates(int vertexCount) {
      reuseTuple = new IdWithCandidates<>();
      boolean[] candidates = new boolean[vertexCount];
      for (int i = 0; i < vertexCount; i++) {
        candidates[i] = true;
      }
      reuseTuple.setCandidates(candidates);
    }

    @Override
    public IdWithCandidates<Long> map(Tuple1<Long> value) throws Exception {
      reuseTuple.setId(value.f0);
      return reuseTuple;
    }
  }

  /**
   * Initializes {@link IdWithCandidates}
   */
  public static class GetTriplesWithCandidates implements
    MapFunction<Tuple3<Long, Long, Long>, TripleWithCandidates<Long>> {
    /**
     * Reduce object instantiations
     */
    private final TripleWithCandidates<Long> reuseTuple;

    /**
     * Constructor
     *
     * @param edgeCount number of query edges
     */
    public GetTriplesWithCandidates(int edgeCount) {
      reuseTuple = new TripleWithCandidates<>();
      boolean[] candidates = new boolean[edgeCount];
      for (int i = 0; i < edgeCount; i++) {
        candidates[i] = true;
      }
      reuseTuple.setCandidates(candidates);
    }

    @Override
    public TripleWithCandidates<Long> map(
      Tuple3<Long, Long, Long> value) throws Exception {
      reuseTuple.setEdgeId(value.f0);
      reuseTuple.setSourceId(value.f1);
      reuseTuple.setTargetId(value.f2);
      return reuseTuple;
    }
  }
}
