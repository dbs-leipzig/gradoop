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

package org.gradoop.flink.model.impl;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.api.operators.GraphTransactionsOperators;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

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
