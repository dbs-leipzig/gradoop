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
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentEdgeFactory;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic configuration for Gradoop Stores
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public abstract class GradoopStoreConfig<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GradoopFlinkConfig {

  /**
   * Graph head handler.
   */
  private final PersistentGraphHeadFactory<G> persistentGraphHeadFactory;
  /**
   * EPGMVertex handler.
   */
  private final PersistentVertexFactory<V, E> persistentVertexFactory;
  /**
   * Edge handler.
   */
  private final PersistentEdgeFactory<E, V> persistentEdgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param persistentGraphHeadFactory  persistent graph head factory
   * @param persistentVertexFactory     persistent vertex factory
   * @param persistentEdgeFactory       persistent edge factory
   * @param env                         Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    PersistentGraphHeadFactory<G> persistentGraphHeadFactory,
    PersistentVertexFactory<V, E> persistentVertexFactory,
    PersistentEdgeFactory<E, V> persistentEdgeFactory,
    ExecutionEnvironment env) {
    this(persistentGraphHeadFactory, persistentVertexFactory, persistentEdgeFactory,
      env,
      new GVEGraphLayoutFactory(),
      new GVECollectionLayoutFactory());
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
    PersistentGraphHeadFactory<G> persistentGraphHeadFactory,
    PersistentVertexFactory<V, E> persistentVertexFactory,
    PersistentEdgeFactory<E, V> persistentEdgeFactory,
    ExecutionEnvironment env,
    LogicalGraphLayoutFactory logicalGraphLayoutFactory,
    GraphCollectionLayoutFactory graphCollectionLayoutFactory) {
    super(env, logicalGraphLayoutFactory, graphCollectionLayoutFactory);

    this.persistentGraphHeadFactory =
      checkNotNull(persistentGraphHeadFactory,
        "PersistentGraphHeadFactory was null");
    this.persistentVertexFactory =
      checkNotNull(persistentVertexFactory,
        "PersistentVertexFactory was null");
    this.persistentEdgeFactory =
      checkNotNull(persistentEdgeFactory,
        "PersistentEdgeFactory was null");
  }

  public PersistentGraphHeadFactory<G> getPersistentGraphHeadFactory() {
    return persistentGraphHeadFactory;
  }

  public PersistentVertexFactory<V, E> getPersistentVertexFactory() {
    return persistentVertexFactory;
  }

  public PersistentEdgeFactory<E, V> getPersistentEdgeFactory() {
    return persistentEdgeFactory;
  }
}
