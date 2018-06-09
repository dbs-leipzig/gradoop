/**
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
package org.gradoop.common.config;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Basic configuration for Gradoop Stores
 *
 * @param <GF> graph head factory
 * @param <VF> vertex factory
 * @param <EF> edge factory
 */
public abstract class GradoopStoreConfig<GF, VF, EF> extends GradoopFlinkConfig {

  /**
   * Graph head handler.
   */
  private final GF persistentGraphHeadFactory;
  /**
   * EPGMVertex handler.
   */
  private final VF persistentVertexFactory;
  /**
   * Edge handler.
   */
  private final EF persistentEdgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param persistentGraphHeadFactory  persistent graph head factory
   * @param persistentVertexFactory     persistent vertex factory
   * @param persistentEdgeFactory       persistent edge factory
   * @param env                         Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    GF persistentGraphHeadFactory,
    VF persistentVertexFactory,
    EF persistentEdgeFactory,
    ExecutionEnvironment env
  ) {
    this(persistentGraphHeadFactory, persistentVertexFactory, persistentEdgeFactory, env,
      new GVEGraphLayoutFactory(), new GVECollectionLayoutFactory());
  }

  /**
   * Creates a new Configuration.
   *
   * @param persistentGraphHeadFactory    persistent graph head factory
   * @param persistentVertexFactory       persistent vertex factory
   * @param persistentEdgeFactory         persistent edge factory
   * @param logicalGraphLayoutFactory     logical graph layout factory
   * @param graphCollectionLayoutFactory  graph collection layout factory
   * @param env                           Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    GF persistentGraphHeadFactory,
    VF persistentVertexFactory,
    EF persistentEdgeFactory,
    ExecutionEnvironment env,
    LogicalGraphLayoutFactory logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory graphCollectionLayoutFactory
  ) {
    super(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory);

    this.persistentGraphHeadFactory = Preconditions.checkNotNull(persistentGraphHeadFactory,
      "GraphHeadFactory was null");
    this.persistentVertexFactory = Preconditions.checkNotNull(persistentVertexFactory,
      "VertexFactory was null");
    this.persistentEdgeFactory = Preconditions.checkNotNull(persistentEdgeFactory,
      "EdgeFactory was null");
  }

  public GF getPersistentGraphHeadFactory() {
    return persistentGraphHeadFactory;
  }

  public VF getPersistentVertexFactory() {
    return persistentVertexFactory;
  }

  public EF getPersistentEdgeFactory() {
    return persistentEdgeFactory;
  }
}
