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

package org.gradoop.flink.algorithms.fsm.config;

import java.io.Serializable;

/**
 * frequent subgraph mining configuration
 */
public class FSMConfig implements Serializable {

  /**
   * Relative support of a subgraph in a graph collection.
   * SubgraphWithCount supported above the minSupport are considered to be frequent.
   */
  private final float minSupport;

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
   * Verbose mode.
   */
  private boolean verbose = false;
  private boolean preprocessing = true;

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

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public boolean usePreprocessing() {
    return preprocessing;
  }
}
