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

/**
 * Representation of an edge on the storage level. We additionally store
 * vertex label information which enables filter mechanisms during loading.
 *
 * @param <V> EPGM vertex type
 */
public interface PersistentEdge<V extends EPGMVertex> extends EPGMEdge {

  /**
   * Loads the vertex data associated with the source vertex.
   *
   * @return source vertex data
   */
  V getSource();

  /**
   * Sets the vertex data associated with the source vertex.
   *
   * @param vertex source vertex data
   */
  void setSource(V vertex);

  /**
   * Loads the vertex data associated with the target vertex.
   *
   * @return target vertex data
   */
  V getTarget();

  /**
   * Sets the vertex data associated with the target vertex.
   *
   * @param vertex target vertex data
   */
  void setTarget(V vertex);

}
