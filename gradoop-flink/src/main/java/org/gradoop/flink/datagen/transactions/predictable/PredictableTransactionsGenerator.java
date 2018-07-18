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
package org.gradoop.flink.datagen.transactions.predictable;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.GraphTransactionsGenerator;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Data generator with predictable result for the evaluation of Frequent
 * SubgraphWithCount Mining algorithms.
 */
public class PredictableTransactionsGenerator implements
  GraphTransactionsGenerator {


  /**
   * specifies the number of generated graphs
   */
  private final long graphCount;
  /**
   * sets the minimum number of embeddings per subgraph pattern.
   */
  private final int graphSize;
  /**
   * sets the graph type: true => multigraph, false => simple graph
   */
  private final boolean multigraph;
  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * constructor
   *
   * @param graphCount number of graphs to generate
   * @param graphSize minimum number of embeddings per subgraph pattern
   * @param multigraph multigraph mode
   * @param config Gradoop configuration
   */
  public PredictableTransactionsGenerator(long graphCount, int graphSize,
    boolean multigraph, GradoopFlinkConfig config) {

    this.graphCount = graphCount;
    this.graphSize = graphSize;
    this.multigraph = multigraph;
    this.config = config;
  }

  @Override
  public DataSet<GraphTransaction> execute() {

    DataSet<Long> graphNumbers = config
      .getExecutionEnvironment()
      .generateSequence(1, graphCount);

    DataSet<GraphTransaction> transactions = graphNumbers
      .map(new PredictableTransaction(graphSize, multigraph, config));

    return transactions;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Returns the number of contained directed frequent subgraphs for a given
   * threshold.
   *
   * @param threshold minimum support
   * @return number of frequent subgraphs
   */
  public static long containedDirectedFrequentSubgraphs(float threshold) {

    return ((Math.round((1.0 - threshold) / 0.1)) + 1) * 702L;
  }

  /**
   * Returns the number of contained undirected frequent subgraphs for a given
   * threshold.
   *
   * @param threshold minimum support
   * @return number of frequent subgraphs
   */
  public static long containedUndirectedFrequentSubgraphs(float threshold) {

    return ((Math.round((1.0 - threshold) / 0.1)) + 1) * 336L;
  }
}
