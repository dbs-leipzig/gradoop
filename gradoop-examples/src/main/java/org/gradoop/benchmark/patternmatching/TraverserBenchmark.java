package org.gradoop.benchmark.patternmatching;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

/**
 * Base class for traverser benchmarks
 */
abstract class TraverserBenchmark extends AbstractRunner {
  /**
   * Path to input graph data
   */
  private final String inputPath;
  /**
   * Number of query vertices
   */
  private int vertexCount;
  /**
   * Number of query edges
   */
  private int edgeCount;
  /**
   * Traversal code to process the query.
   */
  private TraversalCode tc;

  /**
   * Constructor
   *
   * @param inputPath path to input graph data
   */
  TraverserBenchmark(String inputPath) {
    this.inputPath = inputPath;
  }

  /**
   * Initialize the benchmark using a given query. The query can be a predefined one (e.g. q0) or
   * a GDL pattern (e.g. (a)-->(b)).
   *
   * @param queryString user given query string
   */
  void initialize(String queryString) {
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
  }

  int getVertexCount() {
    return vertexCount;
  }

  int getEdgeCount() {
    return edgeCount;
  }

  TraversalCode getTraversalCode() {
    return tc;
  }

  String getInputPath() {
    return inputPath;
  }

  /**
   * Run the benchmark.
   */
  abstract void run() throws Exception;

  /**
   * Returns the query based on the input string
   *
   * @param queryString query identifier (q1, q2, ..., q7)
   * @return query
   */
  private static Queries.Query getQuery(String queryString) {
    Queries.Query query;
    switch (queryString) {
    case "q0":
      query = Queries.q0();
      break;
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
    case "q8":
      query = Queries.q8();
      break;
    case "q9":
      query = Queries.q9();
      break;
    default:
      throw new IllegalArgumentException("unsupported query: " + queryString);
    }
    return query;
  }

  /**
   * Initializes {@link IdWithCandidates}
   */
  @FunctionAnnotation.ForwardedFields("f0")
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
  @FunctionAnnotation.ForwardedFields("f0;f1;f2")
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
