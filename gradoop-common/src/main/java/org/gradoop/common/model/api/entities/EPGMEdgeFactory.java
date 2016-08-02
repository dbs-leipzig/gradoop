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

package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Initializes {@link EPGMEdge} objects of a given type.
 *
 * @param <E> EPGM edge type
 */
public interface EPGMEdgeFactory<E extends EPGMEdge>
  extends EPGMElementFactory<E> {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E createEdge(GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  E initEdge(GradoopId id, GradoopId sourceVertexId, GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    PropertyList properties);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, PropertyList properties);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E createEdge(String label, GradoopId sourceVertexId, GradoopId targetVertexId,
    PropertyList properties, GradoopIdSet graphIds);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id              edge identifier
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param properties      edge properties
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  E initEdge(GradoopId id, String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, PropertyList properties, GradoopIdSet graphIds);
}
