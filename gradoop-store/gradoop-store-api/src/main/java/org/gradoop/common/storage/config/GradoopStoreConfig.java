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
package org.gradoop.common.storage.config;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.storage.api.EPGMConfigProvider;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.annotation.Nonnull;

/**
 * Definition of Gradoop Store Configuration
 *
 * @param <GF> graph head factory
 * @param <VF> vertex factory
 * @param <EF> edge factory
 * @see EPGMConfigProvider store configuration holder
 */
public abstract class GradoopStoreConfig<GF, VF, EF> extends GradoopFlinkConfig {

  /**
   * Graph head factory.
   */
  private final GF storeGraphHeadFactory;
  /**
   * Vertex factory.
   */
  private final VF storeVertexFactory;
  /**
   * Edge factory.
   */
  private final EF storeEdgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param storeGraphHeadFactory  store graph head factory
   * @param storeVertexFactory     store vertex factory
   * @param storeEdgeFactory       store edge factory
   * @param env                    Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    @Nonnull GF storeGraphHeadFactory,
    @Nonnull VF storeVertexFactory,
    @Nonnull EF storeEdgeFactory,
    @Nonnull ExecutionEnvironment env
  ) {
    this(storeGraphHeadFactory,
      storeVertexFactory,
      storeEdgeFactory, env,
      new GVEGraphLayoutFactory(),
      new GVECollectionLayoutFactory());
  }

  /**
   * Creates a new Configuration with layout factory.
   *
   * @param storeGraphHeadFactory         store graph head factory
   * @param storeVertexFactory            store vertex factory
   * @param storeEdgeFactory              store edge factory
   * @param logicalGraphLayoutFactory     logical graph layout factory
   * @param graphCollectionLayoutFactory  graph collection layout factory
   * @param env                           Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    @Nonnull GF storeGraphHeadFactory,
    @Nonnull VF storeVertexFactory,
    @Nonnull EF storeEdgeFactory,
    @Nonnull ExecutionEnvironment env,
    @Nonnull LogicalGraphLayoutFactory logicalGraphLayoutFactory,
    @Nonnull GraphCollectionLayoutFactory graphCollectionLayoutFactory
  ) {
    super(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory);
    this.storeGraphHeadFactory = storeGraphHeadFactory;
    this.storeVertexFactory = storeVertexFactory;
    this.storeEdgeFactory = storeEdgeFactory;
  }

  public GF getStoreGraphHeadFactory() {
    return storeGraphHeadFactory;
  }

  public VF getStoreVertexFactory() {
    return storeVertexFactory;
  }

  public EF getStoreEdgeFactory() {
    return storeEdgeFactory;
  }

}
