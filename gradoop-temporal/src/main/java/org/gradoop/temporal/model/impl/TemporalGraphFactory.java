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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.functions.tpgm.EdgeToTemporalEdge;
import org.gradoop.temporal.model.impl.functions.tpgm.GraphHeadToTemporalGraphHead;
import org.gradoop.temporal.model.impl.functions.tpgm.VertexToTemporalVertex;
import org.gradoop.temporal.model.impl.layout.TemporalGraphLayoutFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.util.Collection;
import java.util.Map;

/**
 * Responsible for creating instances of {@link TemporalGraph} based on a specific layout.
 */
public class TemporalGraphFactory implements
  BaseGraphFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection> {
  /**
   * The factory to create a temporal layout.
   */
  private LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> layoutFactory;
  /**
   * Temporal Gradoop configuration.
   */
  private final TemporalGradoopConfig config;

  /**
   * Creates a new temporal graph factory instance.
   *
   * @param config the temporal Gradoop config
   */
  public TemporalGraphFactory(TemporalGradoopConfig config) {
    this.config = Preconditions.checkNotNull(config);
    this.layoutFactory = new TemporalGraphLayoutFactory();
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> getLayoutFactory() {
    return layoutFactory;
  }

  @Override
  public void setLayoutFactory(
    LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> layoutFactory) {
    this.layoutFactory = layoutFactory;
  }

  @Override
  public TemporalGraph fromDataSets(DataSet<TemporalVertex> vertices) {
    return new TemporalGraph(this.layoutFactory.fromDataSets(vertices), config);
  }

  @Override
  public TemporalGraph fromDataSets(DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    return new TemporalGraph(this.layoutFactory.fromDataSets(vertices, edges), config);
  }

  @Override
  public TemporalGraph fromDataSets(DataSet<TemporalGraphHead> graphHead,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    return new TemporalGraph(this.layoutFactory.fromDataSets(graphHead, vertices, edges), config);
  }

  @Override
  public TemporalGraph fromIndexedDataSets(Map<String, DataSet<TemporalVertex>> vertices,
    Map<String, DataSet<TemporalEdge>> edges) {
    return new TemporalGraph(this.layoutFactory.fromIndexedDataSets(vertices, edges), config);
  }

  @Override
  public TemporalGraph fromIndexedDataSets(Map<String, DataSet<TemporalGraphHead>> graphHead,
    Map<String, DataSet<TemporalVertex>> vertices, Map<String, DataSet<TemporalEdge>> edges) {
    return new TemporalGraph(this.layoutFactory.fromIndexedDataSets(graphHead, vertices, edges),
      config);
  }

  @Override
  public TemporalGraph fromCollections(TemporalGraphHead graphHead,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    return new TemporalGraph(this.layoutFactory.fromCollections(graphHead, vertices, edges),
      config);
  }

  @Override
  public TemporalGraph fromCollections(Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    return new TemporalGraph(this.layoutFactory.fromCollections(vertices, edges), config);
  }

  @Override
  public TemporalGraph createEmptyGraph() {
    return new TemporalGraph(this.layoutFactory.createEmptyGraph(), config);
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
   * Creates a {@link TemporalGraph} instance by the given EPGM graph head, vertex and edge datasets.
   *
   * The method assumes that the given vertices and edges are already assigned to the given
   * graph head.
   *
   * @param graphHead   1-element graph head DataSet
   * @param vertices    vertex DataSet
   * @param edges       edge DataSet
   * @return a temporal graph with default temporal data
   */
  public TemporalGraph fromNonTemporalDataSets(
    DataSet<? extends GraphHead> graphHead,
    DataSet<? extends Vertex> vertices,
    DataSet<? extends Edge> edges) {
    return new TemporalGraph(this.layoutFactory.fromDataSets(
      graphHead.map(new GraphHeadToTemporalGraphHead<>(getGraphHeadFactory())),
      vertices.map(new VertexToTemporalVertex<>(getVertexFactory())),
      edges.map(new EdgeToTemporalEdge<>(getEdgeFactory()))), config);
  }

  /**
   * Creates a {@link TemporalGraph} instance. By the provided timestamp extractors, it is possible
   * to extract temporal information from the data to define a timestamp or time interval that
   * represents the beginning and end of the element's validity (valid time).
   *
   * The method assumes that the given vertices and edges are already assigned to the given graph head.
   *
   * @param graphHead 1-element graph head DataSet
   * @param graphHeadTimeIntervalExtractor extractor to pick the time interval from graph heads
   * @param vertices vertex DataSet
   * @param vertexTimeIntervalExtractor extractor to pick the time interval from vertices
   * @param edges edge DataSet
   * @param edgeTimeIntervalExtractor extractor to pick the time interval from edges
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @return a temporal graph with temporal data extracted using extractor functions
   */
  public <G extends GraphHead, V extends Vertex, E extends Edge> TemporalGraph fromNonTemporalDataSets(
    DataSet<G> graphHead,
    TimeIntervalExtractor<G> graphHeadTimeIntervalExtractor,
    DataSet<V> vertices,
    TimeIntervalExtractor<V> vertexTimeIntervalExtractor,
    DataSet<E> edges,
    TimeIntervalExtractor<E> edgeTimeIntervalExtractor) {

    return new TemporalGraph(this.layoutFactory.fromDataSets(
      graphHead.map(new GraphHeadToTemporalGraphHead<>(getGraphHeadFactory(),
        graphHeadTimeIntervalExtractor)),
      vertices.map(new VertexToTemporalVertex<>(getVertexFactory(),
        vertexTimeIntervalExtractor)),
      edges.map(new EdgeToTemporalEdge<>(getEdgeFactory(),
        edgeTimeIntervalExtractor))), config);
  }

  /**
   * Creates a {@link TemporalGraph} instance from a (non-temporal) base graph.
   *
   * This method calls {@link #fromNonTemporalDataSets(DataSet, DataSet, DataSet)} on the graphs element
   * data sets.
   *
   * @param graph Some base graph.
   * @return The resulting temporal graph.
   */
  public TemporalGraph fromNonTemporalGraph(BaseGraph<?, ?, ?, ?, ?> graph) {
    return fromNonTemporalDataSets(graph.getGraphHead(), graph.getVertices(), graph.getEdges());
  }
}
