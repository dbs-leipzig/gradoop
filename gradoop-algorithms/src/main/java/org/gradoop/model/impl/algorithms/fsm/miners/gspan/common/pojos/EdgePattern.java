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

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos;

/**
 * pojo representing an edge pattern
 *
 * @param <L> label type
 */
public class EdgePattern<L extends Comparable<L>> {

  /**
   * smaller of both vertex labels
   */
  private final L minVertexLabel;
  /**
   * true, smaller label is source label and edge represent edge in direction,
   * false, otherwise
   */
  private final Boolean minToMaxInDirection;
  /**
   * edge label
   */
  private final L edgeLabel;
  /**
   * larger of both vertes labels
   */
  private final L maxVertexLabel;

  /**
   * constructor
   * @param fromVertexLabel traversal start vertex label
   * @param outgoing true, if traversal in direction
   * @param edgeLabel edge label
   * @param maxVertexLabel traversal end vertex label
   */
  public EdgePattern(L fromVertexLabel,
    boolean outgoing, L edgeLabel, L maxVertexLabel) {

    Boolean minToMax = fromVertexLabel.compareTo(maxVertexLabel) <= 0;

    this.minVertexLabel = minToMax ? fromVertexLabel : maxVertexLabel;
    this.minToMaxInDirection = minToMax == outgoing;
    this.edgeLabel = edgeLabel;
    this.maxVertexLabel = minToMax ? maxVertexLabel : fromVertexLabel;
  }

  public L getMinVertexLabel() {
    return minVertexLabel;
  }

  public Boolean isOutgoing() {
    return minToMaxInDirection;
  }

  public L getEdgeLabel() {
    return edgeLabel;
  }

  public L getMaxVertexLabel() {
    return maxVertexLabel;
  }
}
