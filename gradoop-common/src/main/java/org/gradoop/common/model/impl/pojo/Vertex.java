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

package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * POJO Implementation of an EPGM vertex.
 */
public class Vertex extends GraphElement implements EPGMVertex {

  /**
   * Default constructor.
   */
  public Vertex() {
  }

  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphs     graphs that vertex is contained in
   */
  Vertex(final GradoopId id, final String label, final PropertyList properties,
    final GradoopIdSet graphs) {
    super(id, label, properties, graphs);
  }

  @Override
  public String toString() {
    return String.format("(%s)", super.toString());
  }
}
