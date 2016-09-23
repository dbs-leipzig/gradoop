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

package org.gradoop.flink.algorithms.fsm.common.config;

import org.gradoop.flink.algorithms.fsm.common.canonicalization.CAMLabeler;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.CanonicalLabeler;

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
   * Maximum subgraph size by edge count.
   */
  private int maxEdgeCount;

  /**
   * Minimum subgraph size by edge count.
   */
  private int minEdgeCount;

  /**
   * flag to enable preprocessing (true=enabled)
   */
  private final boolean preprocessing;

  /**
   * labeler used to generate canonical labels
   */
  private final CanonicalLabeler canonicalLabeler;

  /**
   * Strategy used to filter embeddings by frequent subgraphs
   */
  private final FilterStrategy filterStrategy;
  private TFSMImplementation implementation;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public FSMConfig(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.maxEdgeCount = 16;
    this.minEdgeCount = 1;
    this.canonicalLabeler =  new CAMLabeler(directed);
    this.preprocessing = true;
    this.filterStrategy = FilterStrategy.BROADCAST_JOIN;
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

  /**
   * Getter for preprocessing flag.
   *
   * @return true, if preprocessing is enabled
   */
  public boolean usePreprocessing() {
    return preprocessing;
  }

  public CanonicalLabeler getCanonicalLabeler() {
    return canonicalLabeler;
  }

  public FilterStrategy getFilterStrategy() {
    return filterStrategy;
  }

  public TFSMImplementation getImplementation() {
    return implementation;
  }
}
