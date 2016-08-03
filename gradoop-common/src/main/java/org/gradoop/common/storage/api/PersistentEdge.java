/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
