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

package org.gradoop.flink.model.impl.operators.matching.common.query;

import java.io.Serializable;

/**
 * Class representing a single step in a {@link TraversalCode}.
 */
public class Step implements Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Long id of the starting vertex of this step
   */
  private long from;

  /**
   * Long id of the edge this step traverses
   */
  private long via;

  /**
   * Long id of the target vertex of this step
   */
  private long to;

  /**
   * Boolean containing if the traversed edge was outgoing from starting
   * vertex
   */
  private boolean isOutgoing;

  /**
   * Creates a new step.
   *
   * @param from starting vertex id
   * @param via traversed edge id
   * @param to target vertex id
   * @param isOutgoing if traversed edge was outgoing from starting vertex
   */
  Step(long from, long via, long to, boolean isOutgoing) {
    this.from = from;
    this.via = via;
    this.to = to;
    this.isOutgoing = isOutgoing;
  }

  /**
   * Returns the Long id of the starting vertex of this step.
   *
   * @return starting vertex id
   */
  public long getFrom() {
    return from;
  }

  /**
   * Returns the Long id of the traversed edge of this step.
   *
   * @return traversed edge id
   */
  public long getVia() {
    return via;
  }

  /**
   * Returns the target vertex of this step.
   *
   * @return target vertex id
   */
  public long getTo() {
    return to;
  }

  /**
   * Returns true if the traversed edge was outgoing.
   *
   * @return if traversed edge was outgoing from starting vertex
   */
  public boolean isOutgoing() {
    return isOutgoing;
  }

  @Override
  public String toString() {
    return String.format("(%d,%d,%d,%s)", from, via, to, isOutgoing);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Step step = (Step) o;

    if (from != step.from) {
      return false;
    }
    if (via != step.via) {
      return false;
    }
    if (to != step.to) {
      return false;
    }
    return isOutgoing == step.isOutgoing;

  }

  @Override
  public int hashCode() {
    int result = (int) (from ^ (from >>> 32));
    result = 31 * result + (int) (via ^ (via >>> 32));
    result = 31 * result + (int) (to ^ (to >>> 32));
    result = 31 * result + (isOutgoing ? 1 : 0);
    return result;
  }
}
