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
package org.gradoop.flink.model.impl.tpgm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.functions.TimeIntervalExtractor;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalEdgeFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalGraphHeadFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalVertexFromNonTemporal;
import org.gradoop.flink.model.impl.layouts.gve.temporal.TemporalGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * Responsible for creating instances of {@link TemporalGraph} based on a specific layout.
 */
public class TemporalGraphFactory
  implements BaseGraphFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph> {
  /**
   * The factory to create a temporal layout.
   */
  private LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> layoutFactory;
  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new temporal graph factory instance.
   *
   * @param config the gradoop flink config
   */
  public TemporalGraphFactory(GradoopFlinkConfig config) {
    this.config = Preconditions.checkNotNull(config);
    this.layoutFactory = new TemporalGraphLayoutFactory();
    this.layoutFactory.setGradoopFlinkConfig(config);
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
  public EPGMGraphHeadFactory<TemporalGraphHead> getGraphHeadFactory() {
    return this.config.getTemporalGraphHeadFactory();
  }

  @Override
  public EPGMVertexFactory<TemporalVertex> getVertexFactory() {
    return this.config.getTemporalVertexFactory();
  }

  @Override
  public EPGMEdgeFactory<TemporalEdge> getEdgeFactory() {
    return this.config.getTemporalEdgeFactory();
  }

  /**
   * Creates a {@link TemporalGraph} instance by the given
   * EPGM graph head, vertex and edge datasets.
   *
   * The method assumes that the given vertices and edges are already assigned to the given
   * graph head.
   *
   * @param graphHead   1-element EPGM graph head DataSet
   * @param vertices    EPGM vertex DataSet
   * @param edges       EPGM edge DataSet
   * @return a temporal graph representing the temporal graph data
   */
  public TemporalGraph fromNonTemporalDataSets(
    DataSet<GraphHead> graphHead,
    DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new TemporalGraph(this.layoutFactory.fromDataSets(
      graphHead.map(new TemporalGraphHeadFromNonTemporal(getGraphHeadFactory())),
      vertices.map(new TemporalVertexFromNonTemporal(getVertexFactory())),
      edges.map(new TemporalEdgeFromNonTemporal(getEdgeFactory()))), config);
  }

  /**
   * Creates a {@link TemporalGraph} instance. By the provided timestamp extractors, it is possible
   * to extract temporal information from the data to define a timestamp or time interval that
   * represents the beginning and end of the element's validity (valid time).
   *
   * The method assumes that the given vertices and edges are already assigned to the given
   * graph head.
   *
   * @param graphHead 1-element EPGM graph head DataSet
   * @param graphHeadTimeIntervalExtractor extractor to pick the time interval from graph heads
   * @param vertices EPGM vertex DataSet
   * @param vertexTimeIntervalExtractor extractor to pick the time interval from vertices
   * @param edges EPGM edge DataSet
   * @param edgeTimeIntervalExtractor extractor to pick the time interval from edges
   * @return the logical graph represented as temporal graph with defined valid times
   */
  public TemporalGraph fromNonTemporalDataSets(
    DataSet<GraphHead> graphHead,
    TimeIntervalExtractor<GraphHead> graphHeadTimeIntervalExtractor,
    DataSet<Vertex> vertices,
    TimeIntervalExtractor<Vertex> vertexTimeIntervalExtractor,
    DataSet<Edge> edges,
    TimeIntervalExtractor<Edge> edgeTimeIntervalExtractor) {

    return new TemporalGraph(this.layoutFactory.fromDataSets(
      graphHead.map(new TemporalGraphHeadFromNonTemporal(getGraphHeadFactory(),
        graphHeadTimeIntervalExtractor)),
      vertices.map(new TemporalVertexFromNonTemporal(getVertexFactory(),
        vertexTimeIntervalExtractor)),
      edges.map(new TemporalEdgeFromNonTemporal(getEdgeFactory(),
        edgeTimeIntervalExtractor))), config);
  }
}
