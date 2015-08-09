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

package org.gradoop.storage;

import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;

/**
 * Representation of edge data on the storage level. We additionally store
 * vertex label information which enables filter mechanisms during loading.
 *
 * @param <VD> vertex data type (used to create persistent vertex identifiers)
 */
public interface PersistentEdgeData<VD extends VertexData> extends EdgeData {

  /**
   * Loads the vertex data associated with the source vertex.
   *
   * @return source vertex data
   */
  VD getSourceVertexData();

  /**
   * Sets the vertex data associated with the source vertex.
   *
   * @param vertexData source vertex data
   */
  void setSourceVertexData(VD vertexData);

  /**
   * Loads the vertex data associated with the target vertex.
   *
   * @return target vertex data
   */
  VD getTargetVertexData();

  /**
   * Sets the vertex data associated with the target vertex.
   *
   * @param vertexData target vertex data
   */
  void setTargetVertexData(VD vertexData);

}
