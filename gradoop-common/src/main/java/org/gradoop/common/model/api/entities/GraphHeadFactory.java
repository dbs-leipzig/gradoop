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
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Initializes {@link GraphHead} objects of a given type.
 */
public interface GraphHeadFactory {

  /**
   * Creates a new graph head based.
   *
   * @return graph data
   */
  GraphHead createGraphHead();

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  GraphHead initGraphHead(GradoopId id);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label graph label
   * @return graph data
   */
  GraphHead createGraphHead(String label);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  GraphHead initGraphHead(GradoopId id, String label);

  /**
   * Creates a new graph head based on the given parameters.
   *
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  GraphHead createGraphHead(String label, PropertyList properties);

  /**
   * Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  GraphHead initGraphHead(GradoopId id, String label, PropertyList properties);

  /**
   * Return the type of the objects created by that factory.
   *
   * @return object type
   */
  Class<? extends GraphHead> getType();
}
