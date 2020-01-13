/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating instances of {@link GraphCollection} based on a specific
 * {@link GraphCollectionLayout}.
 */
public class GraphCollectionFactory implements
  BaseGraphCollectionFactory<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> {
  /**
   * Creates the layout from given data.
   */
  private GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> layoutFactory;

  /**
   * The Gradoop Flink configuration.
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
  public GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> getLayoutFactory() {
    return layoutFactory;
  }

  @Override
  public void setLayoutFactory(
    GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public GraphHeadFactory<EPGMGraphHead> getGraphHeadFactory() {
    return layoutFactory.getGraphHeadFactory();
  }

  @Override
  public VertexFactory<EPGMVertex> getVertexFactory() {
    return layoutFactory.getVertexFactory();
  }

  @Override
  public EdgeFactory<EPGMEdge> getEdgeFactory() {
    return layoutFactory.getEdgeFactory();
  }

  @Override
  public GraphCollection fromDataSets(DataSet<EPGMGraphHead> graphHeads,
    DataSet<EPGMVertex> vertices) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices), config);
  }

  @Override
  public GraphCollection fromDataSets(DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges) {
    return new GraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges), config);
  }

  @Override
  public GraphCollection fromIndexedDataSets(Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices, Map<String, DataSet<EPGMEdge>> edges) {
    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout = layoutFactory
      .fromIndexedDataSets(graphHeads, vertices, edges);
    return new GraphCollection(layout, config);
  }

  @Override
  public GraphCollection fromCollections(Collection<EPGMGraphHead> graphHeads,
    Collection<EPGMVertex> vertices, Collection<EPGMEdge> edges) {
    return new GraphCollection(layoutFactory.fromCollections(graphHeads, vertices, edges), config);
  }

  @Override
  public GraphCollection fromGraph(
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout) {
    return new GraphCollection(layoutFactory.fromGraphLayout(logicalGraphLayout), config);
  }

  @Override
public GraphCollection fromGraphs(
  LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge>... logicalGraphLayouts) {
    if (logicalGraphLayouts.length != 0) {
      DataSet<EPGMGraphHead> graphHeads = null;
      DataSet<EPGMVertex> vertices = null;
      DataSet<EPGMEdge> edges = null;

      if (logicalGraphLayouts.length == 1) {
        return fromGraph(logicalGraphLayouts[0]);
      }

      for (LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraph : logicalGraphLayouts) {
        graphHeads = (graphHeads == null) ?
          logicalGraph.getGraphHead() : graphHeads.union(logicalGraph.getGraphHead());
        vertices = (vertices == null) ?
          logicalGraph.getVertices() : vertices.union(logicalGraph.getVertices());
        edges = (edges == null) ?
          logicalGraph.getEdges() : edges.union(logicalGraph.getEdges());
      }
      return fromDataSets(
        graphHeads.distinct(new Id<>()),
        vertices.distinct(new Id<>()),
        edges.distinct(new Id<>()));
    }
    return createEmptyCollection();
  }

  @Override
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions) {
    return new GraphCollection(layoutFactory.fromTransactions(transactions), config);
  }

  @Override
  public GraphCollection fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<EPGMVertex, EPGMVertex> vertexMergeReducer,
    GroupReduceFunction<EPGMEdge, EPGMEdge> edgeMergeReducer) {
    return new GraphCollection(layoutFactory
      .fromTransactions(transactions, vertexMergeReducer, edgeMergeReducer), config);
  }

  @Override
  public GraphCollection createEmptyCollection() {
    return new GraphCollection(layoutFactory.createEmptyCollection(), config);
  }
}
