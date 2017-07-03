
package org.gradoop.flink.datagen.transactions.predictable;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.operators.GraphTransactionsGenerator;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.GraphTransaction;
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
  public GraphTransactions execute() {

    DataSet<Long> graphNumbers = config
      .getExecutionEnvironment()
      .generateSequence(1, graphCount);

    DataSet<GraphTransaction> transactions = graphNumbers
      .map(new PredictableTransaction(graphSize, multigraph, config));

    return new GraphTransactions(transactions, config);
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
