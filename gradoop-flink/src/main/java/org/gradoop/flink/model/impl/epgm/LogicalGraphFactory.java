/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for creating instances of {@link LogicalGraph} based on a specific
 * {@link org.gradoop.flink.model.api.layouts.LogicalGraphLayout}.
 */
public class LogicalGraphFactory
  implements BaseGraphFactory<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> {
  /**
   * Creates the layout from given data.
   */
  private LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> layoutFactory;
  /**
   * The Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new factory.
   *
   * @param config the Gradoop Flink configuration
   */
  public LogicalGraphFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  @Override
  public void setLayoutFactory(LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  /**
   * {@inheritDoc}
   *
   * The factory is passed from {@link GradoopFlinkConfig} at the moment.
   */
  @Override
  public EPGMGraphHeadFactory<EPGMGraphHead> getGraphHeadFactory() {
    return config.getGraphHeadFactory();
  }

  /**
   * {@inheritDoc}
   *
   * The factory is passed from {@link GradoopFlinkConfig} at the moment.
   */
  @Override
  public EPGMVertexFactory<EPGMVertex> getVertexFactory() {
    return config.getVertexFactory();
  }

  /**
   * {@inheritDoc}
   *
   * The factory is passed from {@link GradoopFlinkConfig} at the moment.
   */
  @Override
  public EPGMEdgeFactory<EPGMEdge> getEdgeFactory() {
    return config.getEdgeFactory();
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<EPGMVertex> vertices) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices), config);
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices, edges), config);
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<EPGMGraphHead> graphHead, DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(graphHead, vertices, edges), config);
  }

  @Override
  public LogicalGraph fromIndexedDataSets(Map<String, DataSet<EPGMVertex>> vertices,
    Map<String, DataSet<EPGMEdge>> edges) {
    return new LogicalGraph(layoutFactory.fromIndexedDataSets(vertices, edges), config);
  }

  @Override
  public LogicalGraph fromIndexedDataSets(Map<String, DataSet<EPGMGraphHead>> graphHead,
    Map<String, DataSet<EPGMVertex>> vertices, Map<String, DataSet<EPGMEdge>> edges) {
    return new LogicalGraph(layoutFactory.fromIndexedDataSets(graphHead, vertices, edges), config);
  }

  @Override
  public LogicalGraph fromCollections(EPGMGraphHead graphHead, Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(graphHead, vertices, edges), config);
  }

  @Override
  public LogicalGraph fromCollections(Collection<EPGMVertex> vertices, Collection<EPGMEdge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(vertices, edges), config);
  }

  @Override
  public LogicalGraph createEmptyGraph() {
    return new LogicalGraph(layoutFactory.createEmptyGraph(), config);
  }
}
