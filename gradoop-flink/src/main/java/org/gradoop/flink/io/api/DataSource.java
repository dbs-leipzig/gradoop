
package org.gradoop.flink.io.api;

import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;

/**
 * Data source in analytical programs.
 */
public interface DataSource {

  /**
   * Reads the input as logical graph.
   *
   * @return logial graph
   */
  LogicalGraph getLogicalGraph() throws IOException;

  /**
   * Reads the input as graph collection.
   *
   * @return graph collection
   */
  GraphCollection getGraphCollection() throws IOException;

  /**
   * Reads the input as graph transactions.
   *
   * @return graph transactions
   */
  GraphTransactions getGraphTransactions() throws IOException;
}
