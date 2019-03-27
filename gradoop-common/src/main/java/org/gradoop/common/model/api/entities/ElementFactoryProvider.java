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
package org.gradoop.common.model.api.entities;

/**
 * Interface that provides getters for the EPGM element factories.
 *
 * @param <G> type of the EPGM graph head
 * @param <V> type of the EPGM vertex
 * @param <E> type of the EPGM edge
 */
public interface ElementFactoryProvider<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> {

  /**
   * Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  EPGMGraphHeadFactory<G> getGraphHeadFactory();

  /**
   * Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  EPGMVertexFactory<V> getVertexFactory();

  /**
   * Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  EPGMEdgeFactory<E> getEdgeFactory();
}
