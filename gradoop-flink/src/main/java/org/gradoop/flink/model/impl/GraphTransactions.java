/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
