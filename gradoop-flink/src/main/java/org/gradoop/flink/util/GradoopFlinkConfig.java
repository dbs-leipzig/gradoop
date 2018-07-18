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
package org.gradoop.flink.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;

import java.util.Objects;

/**
 * Configuration for Gradoop running on Flink.
 */
public class GradoopFlinkConfig extends GradoopConfig<GraphHead, Vertex, Edge> {

  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment executionEnvironment;

  /**
   * Creates instances of {@link org.gradoop.flink.model.api.epgm.LogicalGraph}
   */
  private final LogicalGraphFactory logicalGraphFactory;

  /**
   * Creates instances of {@link org.gradoop.flink.model.api.epgm.GraphCollection}
   */
  private final GraphCollectionFactory graphCollectionFactory;

  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment Flink execution environment
   * @param logicalGraphLayoutFactory Factory for creating logical graphs
   * @param graphCollectionLayoutFactory Factory for creating graph collections
   */
  protected GradoopFlinkConfig(
    ExecutionEnvironment executionEnvironment,
    LogicalGraphLayoutFactory logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory graphCollectionLayoutFactory) {
    super();

    Objects.requireNonNull(executionEnvironment);
    Objects.requireNonNull(logicalGraphLayoutFactory);
    Objects.requireNonNull(graphCollectionLayoutFactory);

    this.executionEnvironment = executionEnvironment;

    // init with default layout factories
    this.logicalGraphFactory = new LogicalGraphFactory(this);
    this.logicalGraphFactory.setLayoutFactory(logicalGraphLayoutFactory);

    this.graphCollectionFactory = new GraphCollectionFactory(this);
    this.graphCollectionFactory.setLayoutFactory(graphCollectionLayoutFactory);
  }

  /**
   * Creates a default Gradoop Flink configuration using POJO handlers.
   *
   * @param env Flink execution environment.
   *
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env) {
    return new GradoopFlinkConfig(env,
      new GVEGraphLayoutFactory(), new GVECollectionLayoutFactory());
  }

  /**
   * Creates a Gradoop Flink configuration using the given parameters.
   *
   * @param env Flink execution environment
   * @param logicalGraphLayoutFactory factory to create logical graph layouts
   * @param graphCollectionLayoutFactory factory to create graph collection layouts
   * @return Gradoop Flink configuration
   */
  public static GradoopFlinkConfig createConfig(ExecutionEnvironment env,
    LogicalGraphLayoutFactory logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory graphCollectionLayoutFactory) {
    return new GradoopFlinkConfig(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory);
  }

  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }

  /**
   * Returns a factory that is able to create logical graph layouts.
   *
   * @return factory for logical graph layouts
   */
  public LogicalGraphFactory getLogicalGraphFactory() {
    return logicalGraphFactory;
  }

  /**
   * Returns a factory that is able to create graph collection layouts.
   *
   * @return factory for graph collection layouts
   */
  public GraphCollectionFactory getGraphCollectionFactory() {
    return graphCollectionFactory;
  }

  /**
   * Sets the layout factory for building layouts that represent a
   * {@link org.gradoop.flink.model.api.epgm.LogicalGraph}.
   *
   * @param factory logical graph layout factor
   */
  public void setLogicalGraphLayoutFactory(LogicalGraphLayoutFactory factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    logicalGraphFactory.setLayoutFactory(factory);
  }

  /**
   * Sets the layout factory for building layouts that represent a
   * {@link org.gradoop.flink.model.api.epgm.GraphCollection}.
   *
   * @param factory graph collection layout factory
   */
  public void setGraphCollectionLayoutFactory(GraphCollectionLayoutFactory factory) {
    Objects.requireNonNull(factory);
    factory.setGradoopFlinkConfig(this);
    graphCollectionFactory.setLayoutFactory(factory);
  }
}
