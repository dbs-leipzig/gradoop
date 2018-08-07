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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.tlf.TLFDataSource;
import org.gradoop.flink.model.impl.operators.matching.transactional.TransactionalPatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.DepthSearchMatching;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.FindEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.GraphTransactionMatcher;
import org.gradoop.flink.model.impl.operators.matching.transactional.function.HasEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;
import org.gradoop.flink.util.GradoopFlinkConfig;
import java.util.concurrent.TimeUnit;

/**
 * Used to benchmark {@link TransactionalPatternMatching} implementation.
 * <p>
 * The benchmarks expects the graph to be stored in two different directories:
 * <p>
 * [inputDir]/vertices -> [unique vertex-id]
 * [inputDir]/edges    -> [unique edge-id,source-vertex-id,target-vertex-id]
 * <p>
 * All identifiers must be of type {@link Long}.
 */
public class TransactionalBenchmark extends AbstractRunner {
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
  private static final String OPTION_RETURN_EMBEDDINGS = "e";

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true, "Graph directory");
    OPTIONS.addOption(OPTION_QUERY, "query", true, "Pattern or fixed query");
    OPTIONS.addOption(OPTION_RETURN_EMBEDDINGS, "embeddings", false,
      "if embeddings should be returned");
  }

  /**
   * Given a query pattern, the benchmark computes the number of matching
   * subgraphs in the given data graph.
   * <p>
   * This benchmark currently supports structure only pattern. For semantic
   * patterns use {@link PatternMatching}.
   * <p>
   * usage: org.gradoop.benchmark.patternmatching.TraverserBenchmark
   * [-i <arg>] [-q <arg>] [-t <arg>]
   * -i,--input <arg>       Graph directory
   * -q,--query <arg>       Pattern or fixed query (e.g. q2 or "(a)-->(b)")
   * -t,--traverser <arg>   [loop|bulk]
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd =
      parseArguments(args, TransactionalBenchmark.class.getName());
    if (cmd == null) {
      return;
    }
    performSanityCheck(cmd);

    String inputPath = cmd.getOptionValue(OPTION_INPUT_PATH);
    String queryString = "query[" + cmd.getOptionValue(OPTION_QUERY) + "]";
    boolean returnEmbeddings = cmd.hasOption(OPTION_RETURN_EMBEDDINGS);

    ExecutionEnvironment env = getExecutionEnvironment();

    TLFDataSource source =
      new TLFDataSource(inputPath, GradoopFlinkConfig.createConfig(env));

    DataSet<GraphWithCandidates> graphs =
      source.getGraphCollection().getGraphTransactions()
        .map(new GraphTransactionMatcher(queryString));

    if (returnEmbeddings) {
      DataSet<Tuple4<GradoopId, GradoopId, GradoopIdSet, GradoopIdSet>> embeddings =
        graphs.flatMap(
          new FindEmbeddings(new DepthSearchMatching(), queryString));

      long embeddingCount = embeddings.count();
      System.out.println("embeddingCount = " + embeddingCount);

    } else {
      DataSet<Tuple2<GradoopId, Boolean>> containment =
        graphs.map(new HasEmbeddings(
          new DepthSearchMatching(), queryString))
          .filter(new SecondFieldTrue<>());

      long containmentCount = containment.count();
      System.out.println(
        "containmentCount = " + containmentCount);
    }

    System.out.println(String.format("Net runtime [s]: %d",
      env
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
    if (!cmd.hasOption(OPTION_QUERY)) {
      throw new IllegalArgumentException("Define an graph output directory.");
    }
  }

  /**
   * Filter that checks if the second field is True.
   * @param <T> any type
   */
  private static class SecondFieldTrue<T> implements
    FilterFunction<Tuple2<T, Boolean>> {
    @Override
    public boolean filter(Tuple2<T, Boolean> tuple2) throws Exception {
      return tuple2.f1;
    }
  }
}
