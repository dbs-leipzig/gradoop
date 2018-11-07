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
package org.gradoop.flink.model.impl.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible to create instances of {@link GraphCollection} based on a specific
 * {@link GraphCollectionLayout}.
 */
public class GraphCollectionFactory
  implements BaseGraphCollectionFactory<GraphHead, Vertex, Edge, GraphCollection> {
  /**
   * Creates the layout from given data.
   */
  private GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> layoutFactory;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new factory.
   *
   * @param config Gradoop Flink configuration
   */
  public GraphCollectionFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  @Override
  public void setLayoutFactory(
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices), config);
  }

  @Override
  public GraphCollection fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges), config);
  }

  @Override
  public GraphCollection fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    GraphCollectionLayout<GraphHead, Vertex, Edge> layout = layoutFactory
      .fromIndexedDataSets(graphHeads, vertices, edges);
    return new GraphCollection(layout, config);
  }

  @Override
  public GraphCollection fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges) {
    return new GraphCollection(layoutFactory.fromCollections(graphHeads, vertices, edges), config);
  }

  @Override
  public GraphCollection fromGraph(LogicalGraph logicalGraphLayout) {
    return new GraphCollection(layoutFactory.fromGraphLayout(logicalGraphLayout), config);
  }

  @Override
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions) {
    return new GraphCollection(layoutFactory.fromTransactions(transactions), config);
  }

  @Override
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer) {
    return new GraphCollection(layoutFactory
      .fromTransactions(transactions, vertexMergeReducer, edgeMergeReducer), config);
  }

  @Override
  public GraphCollection createEmptyCollection() {
    return new GraphCollection(layoutFactory.createEmptyCollection(), config);
  }
}
