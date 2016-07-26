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

package org.gradoop.model.impl.algorithms.fsm.config;

import java.io.Serializable;

/**
 * frequent subgraph mining configuration
 */
public class FSMConfig implements Serializable {

  /**
   * Relative support of a subgraph in a graph collection.
   * Subgraph supported above the minSupport are considered to be frequent.
   */
  private final float minSupport;

  /**
   * Threshold for subgraphs to be considered to be likely frequent.
   * Only for filter-refinement approach (ICDE 2014)
   */
  private float likelinessThreshold = 0.05f;

  /**
   * Direction mode, true for directed graphs and false for undirected.
   */
  private final boolean directed;

  /**
   * Maximum subgraph size by edge count.
   */
  private int maxEdgeCount;

  /**
   * Minimum subgraph size by edge count.
   */
  private int minEdgeCount;



  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public FSMConfig(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.maxEdgeCount = 1000;
    this.minEdgeCount = 0;
  }

  public float getMinSupport() {
    return minSupport;
  }

  public boolean isDirected() {
    return directed;
  }

  public int getMaxEdgeCount() {
    return maxEdgeCount;
  }

  public void setMaxEdgeCount(int maxEdgeCount) {
    this.maxEdgeCount = maxEdgeCount;
  }

  public int getMinEdgeCount() {
    return minEdgeCount;
  }

  public void setMinEdgeCount(int minEdgeCount) {
    this.minEdgeCount = minEdgeCount;
  }

  public float getLikelinessThreshold() {
    return likelinessThreshold;
  }

  public void setLikelinessThreshold(float likelinessThreshold) {
    this.likelinessThreshold = likelinessThreshold;
  }
}
