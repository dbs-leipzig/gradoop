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

package org.gradoop.model.impl.algorithms.fsm.gspan.pojos;

import java.io.Serializable;

/**
 * interface for representing a DFS traversal step
 */
public interface DFSStep extends Serializable {

  /**
   * Getter
   *
   * @return discovery time of traversal start vertex
   */
  int getFromTime();

  /**
   * Getter
   *
   * @return label of traversal start vertex
   */
  Integer getFromLabel();

  /**
   * Getter
   *
   * @return true, if edge was traversed in direction
   */
  Boolean isOutgoing();

  /**
   * Getter
   *
   * @return label of the traversed edge
   */
  Integer getEdgeLabel();

  /**
   * Getter
   *
   * @return discovery time of traversal end vertex
   */
  int getToTime();

  /**
   * Getter
   *
   * @return label of traversal end vertex
   */
  Integer getToLabel();

  /**
   * Getter
   *
   * @return true, if fromTime equals toTime
   */
  Boolean isLoop();

  /**
   * Getter
   *
   * @return true, if fromTime before toTime
   */
  Boolean isForward();

  /**
   * Getter
   *
   * @return true, if toTome before fromTime
   */
  Boolean isBackward();

  /**
   * Getter
   *
   * @return minimal vertex label
   */
  int getMinVertexLabel();
}
