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

package org.gradoop.flink.algorithms.fsm.transactional.common;

import java.io.Serializable;

/**
 * Frequent subgraph mining configuration.
 */
public class FSMConfig implements Serializable {

  /**
   * support threshold for subgraphs to be considered to be frequenct
   */
  private final float minSupport;

  /**
   * Direction mode, true for directed graphs and false for undirected.
   */
  private final boolean directed;

  /**
   * Minimum subgraph size by edge count.
   */
  private final int minEdgeCount;

  /**
   * Maximum subgraph size by edge count.
   */
  private final int maxEdgeCount;

  /**
   * flag to enable preprocessingEnbabled (true=enabled)
   */
  private final boolean preprocessingEnbabled;

  /**
   * Constructor.
   *
   * @param minSupport min support
   * @param directed true, for directed mode
   * @param minEdgeCount min number of edges
   * @param maxEdgeCount max number of edges
   * @param preprocessingEnbabled true, to enable preprocessingEnbabled
   */
  public FSMConfig(
    float minSupport,
    boolean directed,
    int minEdgeCount,
    int maxEdgeCount,
    boolean preprocessingEnbabled
  ) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;
    this.preprocessingEnbabled = preprocessingEnbabled;
  }

  /**
   * Constructor.
   *
   * @param minSupport min support
   * @param directed true, for directed mode
   * @param minEdgeCount min number of edges
   * @param maxEdgeCount max number of edges
   */
  public FSMConfig(
    float minSupport,
    boolean directed,
    int minEdgeCount,
    int maxEdgeCount
  ) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;
    this.preprocessingEnbabled = true;
  }

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public FSMConfig(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.minEdgeCount = 1;
    this.maxEdgeCount = 16;
    this.preprocessingEnbabled = true;
  }

  public float getMinSupport() {
    return minSupport;
  }

  public boolean isDirected() {
    return directed;
  }

  public int getMinEdgeCount() {
    return minEdgeCount;
  }

  public int getMaxEdgeCount() {
    return maxEdgeCount;
  }

  /**
   * Getter for preprocessingEnbabled flag.
   *
   * @return true, if preprocessingEnbabled is enabled
   */
  public boolean isPreprocessingEnabled() {
    return preprocessingEnbabled;
  }

}
