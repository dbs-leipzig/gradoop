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
package org.gradoop.flink.model.impl.layouts.transactional;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.BySourceId;
import org.gradoop.flink.model.impl.functions.epgm.ByTargetId;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionTuple;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Represents a {@link org.gradoop.flink.model.api.epgm.GraphCollection} with a single dataset.
 * Each row in the dataset represents a single {@link org.gradoop.flink.model.api.epgm.LogicalGraph}
 * with all its associated vertex and edge data.
 */
public class TxCollectionLayout implements GraphCollectionLayout {

  private final DataSet<GraphTransaction> transactions;

  TxCollectionLayout(DataSet<GraphTransaction> transactions) {
    this.transactions = transactions;
  }

  @Override
  public DataSet<GraphHead> getGraphHeads() {
    return transactions
      .map(new TransactionTuple())
      .map(new TransactionGraphHead());
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return getGraphHeads().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return transactions;
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return transactions
      .map(new TransactionTuple())
      .flatMap(new TransactionVertices());
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return getVertices().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<Edge> getEdges() {
    return transactions
      .map(new TransactionTuple())
      .flatMap(new TransactionEdges());
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return getEdges().filter(new ByLabel<>(label));
  }

  @Override
  public DataSet<Edge> getOutgoingEdges(GradoopId vertexID) {
    return getEdges().filter(new BySourceId<>(vertexID));
  }

  @Override
  public DataSet<Edge> getIncomingEdges(GradoopId vertexID) {
    return getEdges().filter(new ByTargetId<>(vertexID));
  }
}
