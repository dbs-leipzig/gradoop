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

package org.gradoop.model.impl;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphTransactionsOperators;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Represents a logical graph inside the EPGM.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactions
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphTransactionsOperators<G, V, E> {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<GraphTransaction<G, V, E>> transactions;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Creates a new graph transactions based on the given parameters.
   *
   * @param transactions transaction data set
   * @param config Gradoop Flink configuration
   */
  public GraphTransactions(DataSet<GraphTransaction<G, V, E>> transactions,
    GradoopFlinkConfig<G, V, E> config) {
    this.transactions = transactions;
    this.config = config;
  }

  @Override
  public DataSet<GraphTransaction<G, V, E>> getTransactions() {
    return this.transactions;
  }
}
