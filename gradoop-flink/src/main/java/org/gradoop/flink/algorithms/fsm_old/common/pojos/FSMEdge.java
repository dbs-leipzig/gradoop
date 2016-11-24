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

package org.gradoop.flink.algorithms.fsm_old.common.pojos;

/**
 * Represents an edge within Subgraph embeddings.
 */
public class FSMEdge {

  /**
   * source vertex id
   */
  private final int sourceId;
  /**
   * edge label
   */
  private final String label;
  /**
   * target vertex id
   */
  private final int targetId;

  /**
   * Constructor.
   *
   * @param sourceId source vertex id
   * @param label edge label
   * @param targetId target vertex id
   */
  public FSMEdge(int sourceId, String label, int targetId) {
    this.sourceId = sourceId;
    this.label = label;
    this.targetId = targetId;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getTargetId() {
    return targetId;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FSMEdge that = (FSMEdge) o;

    if (sourceId != that.sourceId) {
      return false;
    }
    if (targetId != that.targetId) {
      return false;
    }
    return label.equals(that.label);

  }

  @Override
  public int hashCode() {
    int result = sourceId;
    result = 31 * result + label.hashCode();
    result = 31 * result + targetId;
    return result;
  }

  @Override
  public String toString() {
    return sourceId + "-" + label + "->" + targetId;
  }
}
