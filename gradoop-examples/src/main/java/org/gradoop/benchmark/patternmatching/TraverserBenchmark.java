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

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.IterationStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.BulkTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.DistributedTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.ForLoopTraverser;

import java.util.concurrent.TimeUnit;

/**
 * Used to benchmark different {@link DistributedTraverser} implementations.
 */
public class TraverserBenchmark implements ProgramDescription {

  /**
   * args[0] - Path to edge list
   * args[1] - Query (q1, q2, ..., q7)
   * args[2] - Iteration strategy (bulk/loop)
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    String inputPath   = args[0];
    String queryString = args[1];
    String itStrategy  = args[2];

    IterationStrategy iterationStrategy = (itStrategy.equals("bulk")) ?
      IterationStrategy.BULK_ITERATION : IterationStrategy.LOOP_UNROLLING;

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // parse query
    // Q?
    Queries.Query query = getQuery(queryString);
    int vertexCount     = query.getVertexCount();
    int edgeCount       = query.getEdgeCount();
    TraversalCode tc    = query.getTraversalCode();
    // GDL
//    QueryHandler query  = new QueryHandler(queryString);
//    int vertexCount     = query.getVertexCount();
//    int edgeCount       = query.getEdgeCount();
//    Traverser traverser = new DFSTraverser();
//    traverser.setQueryHandler(query);
//    TraversalCode tc    = traverser.traverse();

    // read edge list graph
    DataSet<Tuple2<Long, Long>> edgeList = env.readCsvFile(inputPath)
      .fieldDelimiter("\t")
      .ignoreComments("#")
      .types(Long.class, Long.class);

    DataSet<IdWithCandidates<Long>> vertices = edgeList
      .flatMap(new GetVertexIds())
      .distinct()
      .map(new GetIdWithCandidates(vertexCount));

    DataSet<TripleWithCandidates<Long>> edges = DataSetUtils
      .zipWithUniqueId(edgeList)
      .map(new GetTriplesWithCandidates(edgeCount));
    // read edge list graph

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
   * Returns the vertex identifiers from an edge tuple
   */
  public static class GetVertexIds
    implements FlatMapFunction<Tuple2<Long, Long>, Long> {

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Long> out)
      throws Exception {
      out.collect(value.f0);
      out.collect(value.f1);
    }
  }

  /**
   * Initializes {@link IdWithCandidates}
   */
  public static class GetIdWithCandidates
    implements MapFunction<Long, IdWithCandidates<Long>> {
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
    public IdWithCandidates<Long> map(Long value) throws Exception {
      reuseTuple.setId(value);
      return reuseTuple;
    }
  }

  /**
   * Initializes {@link IdWithCandidates}
   */
  public static class GetTriplesWithCandidates implements MapFunction<
      Tuple2<Long, Tuple2<Long, Long>>, TripleWithCandidates<Long>> {
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
      Tuple2<Long, Tuple2<Long, Long>> value) throws Exception {
      reuseTuple.setEdgeId(value.f0);
      reuseTuple.setSourceId(value.f1.f0);
      reuseTuple.setTargetId(value.f1.f1);
      return reuseTuple;
    }
  }

  @Override
  public String getDescription() {
    return TraverserBenchmark.class.getName();
  }
}
