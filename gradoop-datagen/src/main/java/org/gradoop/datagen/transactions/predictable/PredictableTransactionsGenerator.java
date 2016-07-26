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

package org.gradoop.datagen.transactions.predictable;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphTransactionsGenerator;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Data generator with predictable result for the evaluation of Frequent
 * Subgraph Mining algorithms.
 *
 *
 * @param <G> EPGM graph head type.
 * @param <V> EPGM Vertex type.
 * @param <E> EPGM Edge type.
 */
public class PredictableTransactionsGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphTransactionsGenerator<G, V, E> {


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
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * constructor
   *
   * @param graphCount number of graphs to generate
   * @param graphSize minimum number of embeddings per subgraph pattern
   * @param multigraph multigraph mode
   * @param config Gradoop configuration
   */
  public PredictableTransactionsGenerator(long graphCount, int graphSize,
    boolean multigraph, GradoopFlinkConfig<G, V, E> config) {

    this.graphCount = graphCount;
    this.graphSize = graphSize;
    this.multigraph = multigraph;
    this.config = config;
  }

  @Override
  public GraphTransactions<G, V, E> execute() {

    DataSet<Long> graphNumbers = config
      .getExecutionEnvironment()
      .generateSequence(1, graphCount);

    DataSet<GraphTransaction<G, V, E>> transactions = graphNumbers
      .map(new PredictableTransaction<>(graphSize, multigraph, config))
      .returns(GraphTransaction.getTypeInformation(config));

    return new GraphTransactions<>(transactions, config);
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

    return ((Math.round((1.0 - threshold) / 0.1)) + 1) * 230L;
  }
}
