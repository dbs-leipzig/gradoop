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

/**
 * A graph element is part of a logical graph. An element can be part of more
 * than one logical graph. This applies to vertices and edges in the EPGM.
 */
public interface EPGMGraphElement extends EPGMElement {
  /**
   * Returns all graphs that element belongs to.
   *
   * @return all graphs of that element
   */
  GradoopIdSet getGraphIds();

  /**
   * Adds that element to the given graphId. If the element is already an
   * element of the given graphId, nothing happens.
   *
   * @param graphId the graphId to be added to
   */
  void addGraphId(GradoopId graphId);

  /**
   * Adds the given graph set to the element.
   *
   * @param graphIds the graphIds to be added
   */
  void setGraphIds(GradoopIdSet graphIds);

  /**
   * Resets all graph elements.
   */
  void resetGraphIds();

  /**
   * Returns the number of graphs this element belongs to.
   *
   * @return number of graphs containing that element
   */
  int getGraphCount();
}
