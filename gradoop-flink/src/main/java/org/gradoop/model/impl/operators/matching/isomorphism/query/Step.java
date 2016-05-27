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

package org.gradoop.model.impl.operators.matching.isomorphism.query;

/**
 * Class representing a single step in a traversal.
 */
public class Step {

  /**
   * Long id of the starting vertex of this step
   */
  private Long from;

  /**
   * Long id of the edge this step traverses
   */
  private Long via;

  /**
   * Long id of the target vertex of this step
   */
  private Long to;

  /**
   * Boolean containing if the traversed edge was outgoing from starting
   * vertex
   */
  private boolean isOutgoing;

  /**
   * Creates a new step.
   * @param from starting vertex id
   * @param via traversed edge id
   * @param to target vertex id
   * @param isOutgoing if traversed edge was outgoing from starting vertex
   */
  public Step(Long from, Long via, Long to, boolean isOutgoing) {
    this.from = from;
    this.via = via;
    this.to = to;
    this.isOutgoing = isOutgoing;
  }

  /**
   * Returns the Long id of the starting vertex of this step.
   * @return starting vertex id
   */
  public Long getFrom() {
    return from;
  }

  /**
   * Returns the Long id of the traversed edge of this step.
   * @return traversed edge id
   */
  public Long getVia() {
    return via;
  }

  /**
   * Returns the target vertex of this step.
   * @return target vertex id
   */
  public Long getTo() {
    return to;
  }

  /**
   * Returns true if the traversed edge was outgoing.
   * @return if traversed edge was outgoing from starting vertex
   */
  public boolean isOutgoing() {
    return isOutgoing;
  }

  /**
   * Returns a string containing the data. Used for tests.
   * @return string with step data
   */
  public String toString() {
    return from + " " + via + " " + to + " " + isOutgoing;
  }
}
