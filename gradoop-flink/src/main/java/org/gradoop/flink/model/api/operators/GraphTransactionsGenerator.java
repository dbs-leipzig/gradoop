package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.GraphTransactions;

/**
 * Generates a set of graph transactions
 */
public interface GraphTransactionsGenerator extends Operator {

  /**
   * generates the graph transactions
   * @return graph collection
   */
  GraphTransactions execute();
}
