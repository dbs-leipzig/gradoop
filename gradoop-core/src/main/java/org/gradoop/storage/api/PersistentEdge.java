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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

/**
 * Representation of edge data on the storage level. We additionally store
 * vertex label information which enables filter mechanisms during loading.
 *
 * @param <VD> vertex data type (used to create persistent vertex identifiers)
 */
public interface PersistentEdge<VD extends EPGMVertex> extends EPGMEdge {

  /**
   * Loads the vertex data associated with the source vertex.
   *
   * @return source vertex data
   */
  VD getSourceVertex();

  /**
   * Sets the vertex data associated with the source vertex.
   *
   * @param vertex source vertex data
   */
  void setSourceVertex(VD vertex);

  /**
   * Loads the vertex data associated with the target vertex.
   *
   * @return target vertex data
   */
  VD getTargetVertex();

  /**
   * Sets the vertex data associated with the target vertex.
   *
   * @param vertex target vertex data
   */
  void setTargetVertex(VD vertex);

}
