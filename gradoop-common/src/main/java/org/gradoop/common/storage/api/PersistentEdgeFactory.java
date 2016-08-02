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

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 */
public interface PersistentEdgeFactory extends Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdge    edge
   * @param sourceVertex source vertex
   * @param targetVertex target vertex
   * @return persistent edge
   */
  PersistentEdge createEdge(Edge inputEdge, Vertex sourceVertex,
    Vertex targetVertex);
}
