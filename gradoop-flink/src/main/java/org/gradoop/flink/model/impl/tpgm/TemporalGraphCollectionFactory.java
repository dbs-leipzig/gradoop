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
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.functions.TimeIntervalExtractor;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalEdgeFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalGraphHeadFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalVertexFromNonTemporal;
import org.gradoop.flink.model.impl.layouts.gve.temporal.TemporalGraphCollectionLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * Responsible for creating instances of {@link TemporalGraphCollection} based on a specific layout.
 */
public class TemporalGraphCollectionFactory implements BaseGraphCollectionFactory<
  TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraphCollection> {

  /**
   * The factory to create a temporal layout.
   */
  private GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge>
    layoutFactory;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new temporal graph collection factory instance.
   *
   * @param config the gradoop flink config
   */
  public TemporalGraphCollectionFactory(GradoopFlinkConfig config) {
    this.config = Preconditions.checkNotNull(config);
    this.layoutFactory = new TemporalGraphCollectionLayoutFactory();
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  @Override
  public void setLayoutFactory(GraphCollectionLayoutFactory<TemporalGraphHead, TemporalVertex,
    TemporalEdge> factory) {
    this.layoutFactory = factory;
  }

  @Override
  public TemporalGraphCollection fromDataSets(DataSet<TemporalGraphHead> graphHeads,
    DataSet<TemporalVertex> vertices) {
    return new TemporalGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices),
      config);
  }

  @Override
  public TemporalGraphCollection fromDataSets(DataSet<TemporalGraphHead> graphHeads,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    return new TemporalGraphCollection(layoutFactory.fromDataSets(graphHeads, vertices, edges),
      config);
  }

  @Override
  public TemporalGraphCollection fromCollections(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    return new TemporalGraphCollection(layoutFactory.fromCollections(graphHeads, vertices, edges),
      config);
  }

  @Override
  public TemporalGraphCollection fromGraph(
    LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> logicalGraphLayout) {
    return new TemporalGraphCollection(layoutFactory.fromGraphLayout(logicalGraphLayout), config);
  }

  @Override
  public TemporalGraphCollection createEmptyCollection() {
    return new TemporalGraphCollection(layoutFactory.createEmptyCollection(), config);
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
   * Creates a {@link TemporalGraphCollection} instance by the given EPGM graph head, vertex and
   * edge datasets.
   *
   * @param graphHead EPGM graph head DataSet
   * @param vertices EPGM vertex DataSet
   * @param edges EPGM edge DataSet
   * @return a temporal graph collection
   */
  public TemporalGraphCollection fromNonTemporalDataSets(
    DataSet<GraphHead> graphHead,
    DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new TemporalGraphCollection(this.layoutFactory.fromDataSets(
      graphHead.map(new TemporalGraphHeadFromNonTemporal(getGraphHeadFactory())),
      vertices.map(new TemporalVertexFromNonTemporal(getVertexFactory())),
      edges.map(new TemporalEdgeFromNonTemporal(getEdgeFactory()))), config);
  }

  /**
   * Creates a {@link TemporalGraphCollection} instance. By the provided
   * timestamp extractors, it is possible to extract temporal information from the data to
   * define a timestamp or time interval that represents the beginning and end of the element's
   * validity (valid time).
   *
   * @param graphHead EPGM graph head DataSet
   * @param graphHeadTimeIntervalExtractor extractor to pick the time interval from graph heads
   * @param vertices EPGM vertex DataSet
   * @param vertexTimeIntervalExtractor extractor to pick the time interval from vertices
   * @param edges EPGM edge DataSet
   * @param edgeTimeIntervalExtractor extractor to pick the time interval from edges
   * @return the logical graph represented as temporal graph with defined valid times
   */
  public TemporalGraphCollection fromNonTemporalDataSets(
    DataSet<GraphHead> graphHead,
    TimeIntervalExtractor<GraphHead> graphHeadTimeIntervalExtractor,
    DataSet<Vertex> vertices,
    TimeIntervalExtractor<Vertex> vertexTimeIntervalExtractor,
    DataSet<Edge> edges,
    TimeIntervalExtractor<Edge> edgeTimeIntervalExtractor) {

    return new TemporalGraphCollection(this.layoutFactory.fromDataSets(
      graphHead.map(new TemporalGraphHeadFromNonTemporal(getGraphHeadFactory(),
        graphHeadTimeIntervalExtractor)),
      vertices.map(new TemporalVertexFromNonTemporal(getVertexFactory(),
        vertexTimeIntervalExtractor)),
      edges.map(new TemporalEdgeFromNonTemporal(getEdgeFactory(),
        edgeTimeIntervalExtractor))), config);
  }
}
