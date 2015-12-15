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

package org.gradoop.storage.api;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

import java.io.Serializable;

/**
 * Base interface for creating persistent edge data from transient edge data.
 *
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 * @param <PE> output persistent edge data type
 */
public interface PersistentEdgeFactory
  <V extends EPGMVertex, E extends EPGMEdge, PE extends PersistentEdge<V>>
  extends Serializable {

  /**
   * Creates persistent edge data based on the given parameters.
   *
   * @param inputEdge    edge
   * @param sourceVertex source vertex
   * @param targetVertex target vertex
   * @return persistent edge
   */
  PE createEdge(E inputEdge, V sourceVertex, V targetVertex);
}
