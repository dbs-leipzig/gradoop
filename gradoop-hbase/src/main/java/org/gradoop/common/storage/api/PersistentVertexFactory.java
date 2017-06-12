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

import java.io.Serializable;
import java.util.Set;

/**
 * Base interface for creating persistent vertex data from transient vertex
 * data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface PersistentVertexFactory
  <V extends EPGMVertex, E extends EPGMEdge> extends Serializable {

  /**
   * Creates vertex data based on the given parameters.
   *
   * @param inputVertexData input vertex data
   * @param outgoingEdges   outgoing edge identifiers
   * @param incomingEdges   incoming edge identifiers
   * @return persistent vertex data
   */
  PersistentVertex<E> createVertex(V inputVertexData, Set<E> outgoingEdges,
    Set<E> incomingEdges);
}
