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

/**
 * Describes data assigned to an edge in the EPGM.
 */
public interface EPGMEdge extends EPGMGraphElement {
  /**
   * Returns the source vertex identifier.
   *
   * @return source vertex id
   */
  GradoopId getSourceId();

  /**
   * Sets the source vertex identifier.
   *
   * @param sourceId source vertex id
   */
  void setSourceId(GradoopId sourceId);

  /**
   * Returns the target vertex identifier.
   *
   * @return target vertex id
   */
  GradoopId getTargetId();

  /**
   * Sets the target vertex identifier.
   *
   * @param targetId target vertex id.
   */
  void setTargetId(GradoopId targetId);
}
