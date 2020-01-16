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
package org.gradoop.temporal.model.impl;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.functions.tpgm.EdgeToTemporalEdge;
import org.gradoop.temporal.model.impl.functions.tpgm.GraphHeadToTemporalGraphHead;
import org.gradoop.temporal.model.impl.functions.tpgm.VertexToTemporalVertex;
import org.gradoop.temporal.model.impl.layout.TemporalGraphCollectionLayoutFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Responsible for creating instances of {@link TemporalGraphCollection} based on a specific layout.
 */
public class TemporalGraphCollectionFactory implements BaseGraphCollectionFactory<
  TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection> {

  /**
   * The factory to create a temporal layout.
   */
  private GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> layoutFactory;

  /**
   * Temporal Gradoop config.
   */
  private final TemporalGradoopConfig config;

  /**
   * Creates a new temporal graph collection factory instance.
   *
   * @param config the temporal Gradoop config.
   */
  public TemporalGraphCollectionFactory(TemporalGradoopConfig config) {
    this.config = Preconditions.checkNotNull(config);
    this.layoutFactory = new TemporalGraphCollectionLayoutFactory();
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> getLayoutFactory() {
    return layoutFactory;
  }

  @Override
  public void setLayoutFactory(GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex,
    TemporalEdge> factory) {
    this.layoutFactory = factory;
  }

  @Override
  public TemporalGraphCollection fromDataSets(DataSet<TemporalGraphHead> graphHeads,
    DataSet<TemporalVertex> vertices) {
    return new TemporalGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices), config);
  }

  @Override
  public TemporalGraphCollection fromDataSets(DataSet<TemporalGraphHead> graphHeads,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    return new TemporalGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges), config);
  }

  @Override
  public TemporalGraphCollection fromIndexedDataSets(
    Map<String, DataSet<TemporalGraphHead>> graphHeads,
    Map<String, DataSet<TemporalVertex>> vertices,
    Map<String, DataSet<TemporalEdge>> edges) {

    return new TemporalGraphCollection(layoutFactory
      .fromIndexedDataSets(graphHeads, vertices, edges), config);
  }

  @Override
  public TemporalGraphCollection fromCollections(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    return new TemporalGraphCollection(layoutFactory.fromCollections(graphHeads, vertices, edges), config);
  }

  @Override
  public TemporalGraphCollection fromGraph(
    LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> logicalGraphLayout) {
    return new TemporalGraphCollection(layoutFactory.fromGraphLayout(logicalGraphLayout), config);
  }

  @Override
  public TemporalGraphCollection fromGraphs(
    LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>... logicalGraphLayout) {
    if (logicalGraphLayout.length == 0) {
      return createEmptyCollection();
    } else if (logicalGraphLayout.length == 1) {
      return fromGraph(logicalGraphLayout[0]);
    } else {
      return fromDataSets(
        Arrays.stream(logicalGraphLayout).map(LogicalGraphLayout::getGraphHead).reduce(DataSet::union)
          .get().distinct(new Id<>()),
        Arrays.stream(logicalGraphLayout).map(LogicalGraphLayout::getVertices).reduce(DataSet::union)
          .get().distinct(new Id<>()),
        Arrays.stream(logicalGraphLayout).map(LogicalGraphLayout::getEdges).reduce(DataSet::union)
          .get().distinct(new Id<>()));
    }
  }

  @Override
  public TemporalGraphCollection fromTransactions(DataSet<GraphTransaction> transactions) {
    throw new UnsupportedOperationException("This operation is not (yet) supported.");
  }

  @Override
  public TemporalGraphCollection fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<TemporalVertex, TemporalVertex> vertexMergeReducer,
    GroupReduceFunction<TemporalEdge, TemporalEdge> edgeMergeReducer) {
    throw new UnsupportedOperationException("This operation is not (yet) supported.");
  }

  @Override
  public TemporalGraphCollection createEmptyCollection() {
    return new TemporalGraphCollection(layoutFactory.createEmptyCollection(), config);
  }

  @Override
  public GraphHeadFactory<TemporalGraphHead> getGraphHeadFactory() {
    return layoutFactory.getGraphHeadFactory();
  }

  @Override
  public VertexFactory<TemporalVertex> getVertexFactory() {
    return layoutFactory.getVertexFactory();
  }

  @Override
  public EdgeFactory<TemporalEdge> getEdgeFactory() {
    return layoutFactory.getEdgeFactory();
  }

  /**
   * Creates a {@link TemporalGraphCollection} instance by the given graph head, vertex and edge datasets.
   *
   * @param graphHead graph head DataSet
   * @param vertices vertex DataSet
   * @param edges edge DataSet
   * @return a temporal graph collection
   */
  public TemporalGraphCollection fromNonTemporalDataSets(
    DataSet<? extends GraphHead> graphHead,
    DataSet<? extends Vertex> vertices,
    DataSet<? extends Edge> edges) {
    return new TemporalGraphCollection(this.layoutFactory.fromDataSets(
      graphHead.map(new GraphHeadToTemporalGraphHead<>(getGraphHeadFactory())),
      vertices.map(new VertexToTemporalVertex<>(getVertexFactory())),
      edges.map(new EdgeToTemporalEdge<>(getEdgeFactory()))), config);
  }

  /**
   * Creates a {@link TemporalGraphCollection} instance. By the provided timestamp extractors, it is
   * possible to extract temporal information from the data to define a timestamp or time interval that
   * represents the beginning and end of the element's validity (valid time).
   *
   * @param graphHead graph head DataSet
   * @param graphHeadTimeIntervalExtractor extractor to pick the time interval from graph heads
   * @param vertices vertex DataSet
   * @param vertexTimeIntervalExtractor extractor to pick the time interval from vertices
   * @param edges edge DataSet
   * @param edgeTimeIntervalExtractor extractor to pick the time interval from edges
   * @return the logical graph represented as temporal graph with defined valid times
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   */
  public <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> TemporalGraphCollection fromNonTemporalDataSets(
    DataSet<G> graphHead,
    TimeIntervalExtractor<G> graphHeadTimeIntervalExtractor,
    DataSet<V> vertices,
    TimeIntervalExtractor<V> vertexTimeIntervalExtractor,
    DataSet<E> edges,
    TimeIntervalExtractor<E> edgeTimeIntervalExtractor) {

    return new TemporalGraphCollection(this.layoutFactory.fromDataSets(
      graphHead.map(new GraphHeadToTemporalGraphHead<>(getGraphHeadFactory(),
        graphHeadTimeIntervalExtractor)),
      vertices.map(new VertexToTemporalVertex<>(getVertexFactory(),
        vertexTimeIntervalExtractor)),
      edges.map(new EdgeToTemporalEdge<>(getEdgeFactory(),
        edgeTimeIntervalExtractor))), config);
  }

  /**
   * Creates a {@link TemporalGraphCollection} from a (non-temporal) base graph collection.
   *
   * This method calls {@link #fromNonTemporalDataSets(DataSet, DataSet, DataSet)} on the graph collections
   * element data sets.
   *
   * @param collection Some base graph collection.
   * @return The resulting temporal graph collection.
   */
  public TemporalGraphCollection fromNonTemporalGraphCollection(
    BaseGraphCollection<?, ?, ?, ?, ?> collection) {
    return fromNonTemporalDataSets(
      collection.getGraphHeads(), collection.getVertices(), collection.getEdges());
  }
}
