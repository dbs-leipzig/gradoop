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
package org.gradoop.flink.model.impl.layouts.transactional;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.functions.epgm.ByDifferentGraphId;
import org.gradoop.flink.model.impl.functions.epgm.ByDifferentId;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Represents a {@link org.gradoop.flink.model.api.epgm.GraphCollection} with a single dataset.
 * Each row in the dataset represents a single {@link org.gradoop.flink.model.api.epgm.LogicalGraph}
 * with all its associated vertex and edge data.
 */
public class TxCollectionLayout implements GraphCollectionLayout {
  /**
   * Flink dataset holding the actual data of that layout.
   */
  private final DataSet<GraphTransaction> transactions;

  /**
   * Creates a new transactional collection layout.
   *
   * @param transactions graph transactions
   */
  TxCollectionLayout(DataSet<GraphTransaction> transactions) {
    this.transactions = transactions;
  }

  @Override
  public boolean isGVELayout() {
    return false;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public boolean isTransactionalLayout() {
    return true;
  }

  @Override
  public DataSet<GraphHead> getGraphHeads() {
    return transactions
      .map(new TransactionGraphHead<>())
      .filter(new ByDifferentId<>(GradoopConstants.DB_GRAPH_ID));
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return getGraphHeads().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return transactions.filter(new ByDifferentGraphId(GradoopConstants.DB_GRAPH_ID));
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return transactions
      .flatMap(new TransactionVertices<>())
      .distinct(new Id<>());
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return getVertices().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<Edge> getEdges() {
    return transactions
      .flatMap(new TransactionEdges<>())
      .distinct(new Id<>());
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return getEdges().filter(new ByLabel<>(label));
  }
}
