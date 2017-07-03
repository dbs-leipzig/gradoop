
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Describes all operators that can be applied on a single logical graph in the
 * EPGM.
 */
public interface GraphTransactionsOperators {

  /**
   * Getter.
   * @return data set of graph transactions
   */
  DataSet<GraphTransaction> getTransactions();

  /**
   * Getter.
   * @return Gradoop Flink Configuration
   */
  GradoopFlinkConfig getConfig();
}
