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

package org.gradoop.model;

/**
 * A connected entity can have connections to other entities.
 */
public interface Connected {

  /**
   * Returns all connections starting at that entity.
   *
   * @return outgoing edges
   */
  Iterable<Edge> getOutgoingEdges();

  /**
   * Returns all connection ending at that entity.
   *
   * @return incoming edges
   */
  Iterable<Edge> getIncomingEdges();

  /**
   * Returns the number of edges starting at that entity.
   *
   * @return outgoing edge count
   */
  int getOutgoingDegree();

  /**
   * Returns the number of edges ending in that entity.
   *
   * @return incoming edge count
   */
  int getIncomingDegree();

  /**
   * Returns the number of edges connected to that entity.
   *
   * @return total edge count
   */
  int getDegree();
}
