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
package org.gradoop.flink.model.api.epgm;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Default interface of a EPGM graph collection instance.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <GC> the type of the graph collection that will be created with a provided factory
 */
public interface BaseGraphCollection<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  GC extends BaseGraphCollection<G, V, E, GC>> extends GraphCollectionLayout<G, V, E> {
  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return the Gradoop Flink configuration
   */
  GradoopFlinkConfig getConfig();

  /**
   * Get the factory that is responsible for creating an instance of {@link GC}.
   *
   * @return a factory that can be used to create a {@link GC} instance
   */
  BaseGraphCollectionFactory<G, V, E, GC> getFactory();
}
