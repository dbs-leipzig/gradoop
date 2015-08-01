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
 * An edge identifier is unique in the context of a vertex entity. An edge can
 * be either outgoing or incoming. In case of an outgoing edge the edge points
 * to the target of that connection, in case of an incoming edge the edge points
 * to the source of that connection.
 * <p/>
 * An edge has exactly one label and an internal index to handle multiple edges
 * between two vertex instances with the same label.
 */
public interface Edge extends Labeled, Attributed {
  /**
   * Returns the id of the other vertex this edge belongs to. In case of an
   * outgoing edge, this is the target vertex. In case of an incoming edge this
   * is the source vertex of that edge.
   *
   * @return vertex id this edge connects to
   */
  Long getOtherID();

  /**
   * Returns the vertex specific index of that edge.
   *
   * @return vertex specific edge index
   */
  Long getIndex();
}
