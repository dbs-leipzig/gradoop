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
 * Initializes {@link EPGMVertex} objects of a given type.
 *
 * @param <V> EPGM vertex type
 */
public interface EPGMVertexFactory<V extends EPGMVertex>
  extends EPGMElementFactory<V> {

  /**
   * Initializes a new vertex based on the given parameters.
   *
   * @return vertex data
   */
  V createVertex();

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  V initVertex(GradoopId id);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label vertex label
   * @return vertex data
   */
  V createVertex(String label);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  V initVertex(GradoopId id, String label);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  EPGMVertex createVertex(String label, PropertyList properties);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, PropertyList properties);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V createVertex(String label, GradoopIdSet graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id     vertex identifier
   * @param label  vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, GradoopIdSet graphIds);

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  V createVertex(String label, PropertyList properties, GradoopIdSet graphIds);

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex labels
   * @param properties vertex properties
   * @param graphIds     graphIds, that contain the vertex
   * @return vertex data
   */
  V initVertex(GradoopId id, String label, PropertyList properties,
    GradoopIdSet graphIds);
}
