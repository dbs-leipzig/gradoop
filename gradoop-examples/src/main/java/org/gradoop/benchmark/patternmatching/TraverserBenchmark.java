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
package org.gradoop.benchmark.patternmatching;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
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
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Base class for traverser benchmarks
 */
abstract class TraverserBenchmark extends AbstractRunner {
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
  /**
   * Path to CSV log file
   */
  private static final String OPTION_CSV_PATH = "csv";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph directory");
    OPTIONS.addOption(OPTION_QUERY, "query", true, "Pattern or fixed query");
    OPTIONS.addOption(OPTION_TRAVERSER, "traverser", true,
      "[set-pair-for|set-pair-bulk|triple-for]");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv-path", true, "Path to output CSV file");
  }

  /**
   * Path to input graph data
   */
  private final String inputPath;
  /**
   * Query to execute.
   */
  private final String query;
  /**
   * Traverser strategy
   */
  private final TraverserStrategy traverserStrategy;
  /**
   * Path to CSV output
   */
  private String csvPath;

  /**
   * Number of query vertices
   */
  private int vertexCount;
  /**
   * Number of query edges
   */
  private int edgeCount;
  /**
   * Number of embeddings found by the traverser
   */
  private long embeddingCount;
  /**
   * Traversal code to process the query.
   */
  private TraversalCode tc;

  /**
   * Constructor
   *
   * @param cmd commandline
   */
  TraverserBenchmark(CommandLine cmd) {
    this.inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    this.query = cmd.getOptionValue(OPTION_QUERY);

    String traverserStrategyString = cmd.getOptionValue(OPTION_TRAVERSER).toLowerCase();

    switch (traverserStrategyString) {
    case "set-pair-bulk":
      this.traverserStrategy = TraverserStrategy.SET_PAIR_BULK_ITERATION;
      break;
    case "set-pair-for":
      this.traverserStrategy = TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION;
      break;
    case "triple-for":
      this.traverserStrategy = TraverserStrategy.TRIPLES_FOR_LOOP_ITERATION;
      break;
    default:
      throw new IllegalArgumentException("Unknown traverser strategy: " + traverserStrategyString);
    }

    if (cmd.hasOption(OPTION_CSV_PATH)) {
      csvPath = cmd.getOptionValue(OPTION_CSV_PATH);
    }
    initialize();
  }

  /**
   * Initialize the benchmark using a given query. The query can be a predefined one (e.g. q0) or
   * a GDL pattern (e.g. (a)-->(b)).
   */
  private void initialize() {
    if (query.toLowerCase().startsWith("q")) {
      // fixed query
      Queries.Query q = getQuery(query.toLowerCase());
      vertexCount = q.getVertexCount();
      edgeCount = q.getEdgeCount();
      tc = q.getTraversalCode();
    } else {
      // GDL query pattern
      QueryHandler queryHandler = new QueryHandler(query);
      vertexCount = queryHandler.getVertexCount();
      edgeCount = queryHandler.getEdgeCount();
      Traverser traverser = new DFSTraverser();
      traverser.setQueryHandler(queryHandler);
      tc = traverser.traverse();
    }
  }

  /**
   * Returns a string containing information about the benchmark run.
   *
   * @return benchmark result string
   */
  private String getResultString() {
    return String.format("%s|%s|%s|%s|%s|%s",
      inputPath,
      getExecutionEnvironment().getParallelism(),
      traverserStrategy.name(),
      query,
      embeddingCount,
      getExecutionEnvironment().getLastJobExecutionResult().getNetRuntime());
  }

  /**
   * Writes the results of the benchmark into the given csv file. If the file already exists,
   * the results are appended.
   *
   * @param csvFile path to csv file
   * @throws IOException
   */
  private void writeResults(String csvFile) throws IOException {
    String header = "Input|Parallelism|Strategy|Query|Embeddings|Runtime[ms]";
    String line = getResultString();

    File f = new File(csvFile);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, String.format("%s%n", line), true);
    } else {
      PrintWriter writer = new PrintWriter(csvFile, "UTF-8");
      writer.println(header);
      writer.println(line);
      writer.close();
    }
  }

  /**
   * Prints the results of the benchmark to system out.
   */
  private void printResults() {
    System.out.println(getResultString());
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

  void setEmbeddingCount(long embeddingCount) {
    this.embeddingCount = embeddingCount;
  }

  TraverserStrategy getTraverserStrategy() {
    return traverserStrategy;
  }


  /**
   * Run the benchmark.
   */
  abstract void run() throws Exception;

  /**
   * Writes the results to file or prints it.
   *
   * @throws IOException
   */
  void close() throws IOException {
    if (csvPath != null) {
      writeResults(csvPath);
    } else {
      printResults();
    }
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
