
package org.gradoop.flink.model.impl;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.api.operators.GraphTransactionsOperators;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Represents a logical graph inside the EPGM.
 */
public class GraphTransactions implements GraphTransactionsOperators {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<GraphTransaction> transactions;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new graph transactions based on the given parameters.
   *
   * @param transactions transaction data set
   * @param config Gradoop Flink configuration
   */
  public GraphTransactions(DataSet<GraphTransaction> transactions,
    GradoopFlinkConfig config) {
    this.transactions = transactions;
    this.config = config;
  }

  @Override
  public DataSet<GraphTransaction> getTransactions() {
    return this.transactions;
  }

  public GradoopFlinkConfig getConfig() {
    return config;
  }
}
