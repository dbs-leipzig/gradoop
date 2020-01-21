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
package org.gradoop.temporal.util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraphCollectionFactory;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for Gradoop with additional support for temporal graphs.
 */
public class TemporalGradoopConfig extends GradoopFlinkConfig {

  /**
   * The factory used to create new temporal graphs.
   */
  private final TemporalGraphFactory temporalGraphFactory;

  /**
   * The factory used to create new temporal graph collections.
   */
  private final TemporalGraphCollectionFactory temporalGraphCollectionFactory;

  /**
   * Creates a new temporal config from an existing {@link GradoopFlinkConfig}.
   * Use {@link #fromGradoopFlinkConfig(GradoopFlinkConfig)} to use this constructor.
   *
   * @param existingConfig The existing configuration.
   */
  private TemporalGradoopConfig(GradoopFlinkConfig existingConfig) {
    this(requireNonNull(existingConfig).getExecutionEnvironment(),
      existingConfig.getLogicalGraphFactory().getLayoutFactory(),
      existingConfig.getGraphCollectionFactory().getLayoutFactory());
  }

  /**
   * Create a new temporal config that also acts as a regular {@link GradoopFlinkConfig}.
   *
   * @param executionEnvironment        Flink execution environment.
   * @param epgmLayoutFactory           Factory for creating EPGM graphs.
   * @param epgmCollectionLayoutFactory Factory for creating EPGM graph collections.
   */
  private TemporalGradoopConfig(
    ExecutionEnvironment executionEnvironment,
    LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> epgmLayoutFactory,
    GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> epgmCollectionLayoutFactory) {
    super(executionEnvironment, epgmLayoutFactory, epgmCollectionLayoutFactory);
    temporalGraphFactory = new TemporalGraphFactory(this);
    temporalGraphCollectionFactory = new TemporalGraphCollectionFactory(this);
  }

  /**
   * Create a new config from an existing {@link GradoopFlinkConfig}.
   *
   * @param config The existing configuration.
   * @return A new temporal Gradoop config.
   */
  public static TemporalGradoopConfig fromGradoopFlinkConfig(GradoopFlinkConfig config) {
    return new TemporalGradoopConfig(config);
  }

  /**
   * Create a new config.
   *
   * @param executionEnvironment The Flink execution environment.
   * @return A new temporal config.
   */
  public static TemporalGradoopConfig createConfig(ExecutionEnvironment executionEnvironment) {
    return new TemporalGradoopConfig(requireNonNull(executionEnvironment), new GVEGraphLayoutFactory(),
      new GVECollectionLayoutFactory());
  }

  /**
   * Return a factory that is able to create temporal graphs.
   *
   * @return A factory used to create temporal graphs.
   */
  public TemporalGraphFactory getTemporalGraphFactory() {
    return temporalGraphFactory;
  }

  /**
   * Return a factory that is able to create temporal graph collections.
   *
   * @return A factory used to create temporal graph collections.
   */
  public TemporalGraphCollectionFactory getTemporalGraphCollectionFactory() {
    return temporalGraphCollectionFactory;
  }
}
