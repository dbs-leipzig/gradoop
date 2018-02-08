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

import java.util.Set;

/**
 * Representation of vertex data on the storage level. We additionally store
 * outgoing and incoming edges for faster access during e.g. traversal.
 *
 * @param <E> EPGM edge type
 */
public interface PersistentVertex<E extends EPGMEdge> extends EPGMVertex {

  /**
   * Returns outgoing edge data for the vertex.
   *
   * @return outgoing edge data
   */
  Set<E> getOutgoingEdges();

  /**
   * Sets outgoing edge data.
   *
   * @param outgoingEdgeData outgoing edge data
   */
  void setOutgoingEdges(Set<E> outgoingEdgeData);

  /**
   * Returns incoming edge data for the vertex.
   *
   * @return incoming edge data
   */
  Set<E> getIncomingEdges();

  /**
   * Sets incoming edge data.
   *
   * @param incomingEdgeData incoming edge data
   */
  void setIncomingEdges(Set<E> incomingEdgeData);

}
