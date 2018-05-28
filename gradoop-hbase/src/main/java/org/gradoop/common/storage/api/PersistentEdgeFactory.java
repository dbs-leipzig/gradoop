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
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface PersistentEdgeFactory<E extends EPGMEdge, V extends EPGMVertex>
  extends Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdge    edge
   * @param sourceVertex source vertex
   * @param targetVertex target vertex
   * @return persistent edge
   */
  PersistentEdge<V> createEdge(E inputEdge, V sourceVertex, V targetVertex);
}
