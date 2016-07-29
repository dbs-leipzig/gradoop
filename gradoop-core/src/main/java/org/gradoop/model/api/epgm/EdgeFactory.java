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

package org.gradoop.model.api.epgm;

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * Initializes {@link Edge} objects of a given type.
 */
public interface EdgeFactory extends ElementFactory {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  Edge createEdge(GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  Edge initEdge(GradoopId id,
    GradoopId sourceVertexId,
    GradoopId targetVertexId);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  Edge createEdge(String label,
    GradoopId sourceVertexId,
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
  Edge initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
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
  Edge createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
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
  Edge initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties);

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param label           edge label
   * @param sourceVertexId  source vertex id
   * @param targetVertexId  target vertex id
   * @param graphIds        graphIds, that contain the edge
   * @return edge data
   */
  Edge createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
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
  Edge initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    GradoopIdSet graphIds);

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
  Edge createEdge(String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties,
    GradoopIdSet graphIds);

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
  Edge initEdge(GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties,
    GradoopIdSet graphIds);
}
