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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.api;

import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

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
  Set<GradoopId> getGraphs();

  /**
   * Adds that element to the given graph. If the element is already an element
   * of the given graph, nothing happens.
   *
   * @param graph the graph to be added to
   */
  void addGraph(GradoopId graph);

  /**
   * Adds the given graph set to the element.
   *
   * @param graphs the graphs to be added
   */
  void setGraphs(Set<GradoopId> graphs);

  /**
   * Resets all graph elements.
   */
  void resetGraphs();

  /**
   * Returns the number of graphs this element belongs to.
   *
   * @return number of graphs containing that element
   */
  int getGraphCount();
}
