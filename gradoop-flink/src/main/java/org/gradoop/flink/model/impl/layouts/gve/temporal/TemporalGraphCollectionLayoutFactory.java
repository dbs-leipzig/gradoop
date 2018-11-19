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
package org.gradoop.flink.model.impl.layouts.gve.temporal;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating a {@link TemporalGVELayout} from given data.
 */
public class TemporalGraphCollectionLayoutFactory extends BaseFactory implements
  GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalGraphHead> graphHeads, DataSet<TemporalVertex> vertices) {
    return fromDataSets(graphHeads, vertices, createTemporalEdgeDataSet(new ArrayList<>(0)));
  }

  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalGraphHead> graphHeads, DataSet<TemporalVertex> vertices,
    DataSet<TemporalEdge> edges) {
    return new TemporalGVELayout(
      Objects.requireNonNull(graphHeads),
      Objects.requireNonNull(vertices),
      Objects.requireNonNull(edges));
  }

  /**
   * {@inheritDoc}
   *
   * Creating a temporal graph layout from an indexed dataset is not supported yet.
   */
  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromIndexedDataSets(
    Map<String, DataSet<TemporalGraphHead>> graphHeads,
    Map<String, DataSet<TemporalVertex>> vertices, Map<String, DataSet<TemporalEdge>> edges) {
    throw new UnsupportedOperationException(
      "Creating a temporal graph layout from an indexed dataset is not supported yet.");
  }

  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromCollections(
    Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    return fromDataSets(
      createTemporalGraphHeadDataSet(Objects.requireNonNull(graphHeads)),
      createTemporalVertexDataSet(Objects.requireNonNull(vertices)),
      createTemporalEdgeDataSet(Objects.requireNonNull(edges)));
  }

  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromGraphLayout(
    LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> logicalGraphLayout) {
    return fromDataSets(
      logicalGraphLayout.getGraphHead(),
      logicalGraphLayout.getVertices(),
      logicalGraphLayout.getEdges());
  }

  /**
   * {@inheritDoc}
   *
   * Creating a temporal graph layout from graph transactions is not supported yet.
   */
  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromTransactions(
    DataSet<GraphTransaction> transactions) {
    throw new UnsupportedOperationException(
      "Creating a temporal graph layout from graph transactions is not supported yet.");
  }

  /**
   * {@inheritDoc}
   *
   * Creating a temporal graph layout from graph transactions is not supported yet.
   */
  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromTransactions(
    DataSet<GraphTransaction> transactions,
    GroupReduceFunction<TemporalVertex, TemporalVertex> vertexMergeReducer,
    GroupReduceFunction<TemporalEdge, TemporalEdge> edgeMergeReducer) {
    throw new UnsupportedOperationException(
      "Creating a temporal graph layout from graph transactions is not supported yet.");
  }

  @Override
  public GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>
  createEmptyCollection() {
    return fromCollections(new ArrayList<>(0), new ArrayList<>(0), new ArrayList<>(0));
  }
}
