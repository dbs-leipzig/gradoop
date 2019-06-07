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
package org.gradoop.flink.model.api.epgm;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Default interface of a EPGM logical graph instance.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph that will be created with a provided factory
 * @param <GC> the type of the graph collection that will be created with a provided factory
 */
public interface BaseGraph<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends LogicalGraphLayout<G, V, E> {
  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return the Gradoop Flink configuration
   */
  GradoopFlinkConfig getConfig();

  /**
   * Get the factory that is responsible for creating an instance of {@link LG}.
   *
   * @return a factory that can be used to create a {@link LG} instance
   */
  BaseGraphFactory<G, V, E, LG, GC> getFactory();

  /**
   * Get the factory that is responsible for creating an instance of a graph collection of type
   * {@link GC}.
   *
   * @return a factory that can be used to create a {@link GC} instance.
   */
  BaseGraphCollectionFactory<G, V, E, LG, GC> getCollectionFactory();

}
