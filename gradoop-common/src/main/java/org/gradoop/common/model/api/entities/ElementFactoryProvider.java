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
package org.gradoop.common.model.api.entities;

/**
 * Interface that provides getters for the element factories.
 *
 * @param <G> type of the graph head
 * @param <V> type of the vertex
 * @param <E> type of the edge
 */
public interface ElementFactoryProvider<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge> {

  /**
   * Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  GraphHeadFactory<G> getGraphHeadFactory();

  /**
   * Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  VertexFactory<V> getVertexFactory();

  /**
   * Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  EdgeFactory<E> getEdgeFactory();
}
