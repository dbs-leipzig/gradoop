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

import org.gradoop.flink.algorithms.fsm.common.canonicalization.api.CanonicalLabeler;


import org.gradoop.flink.algorithms.fsm.common.canonicalization.cam.CAMLabeler;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.gspan.MinDFSLabeler;

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
   * flag to enable preprocessing (true=enabled)
   */
  private final boolean preprocessing;

  /**
   * labeler used to generate canonical labels
   */
  private final CanonicalLabel canonicalLabel;

  /**
   * Strategy used to filter embeddings by frequent subgraphs
   */
  private final FilterStrategy filterStrategy;

  /**
   * Strategy used to grow children of frequent subgraphs
   */
  private final GrowthStrategy growthStrategy;

  /**
   * Strategy for distributed iteration
   */
  private final IterationStrategy iterationStrategy;

  public FSMConfig(
    float minSupport,
    boolean directed,
    int minEdgeCount,
    int maxEdgeCount,
    boolean preprocessing,
    CanonicalLabel canonicalLabel,
    FilterStrategy filterStrategy,
    GrowthStrategy growthStrategy,
    IterationStrategy iterationStrategy
  ) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;
    this.preprocessing = preprocessing;
    this.canonicalLabel = canonicalLabel;
    this.filterStrategy = filterStrategy;
    this.growthStrategy = growthStrategy;
    this.iterationStrategy = iterationStrategy;
  }

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
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
    this.preprocessing = true;
    this.canonicalLabel = CanonicalLabel.MIN_DFS;
    this.filterStrategy = FilterStrategy.BROADCAST_JOIN;
    this.growthStrategy = GrowthStrategy.FUSION;
    this.iterationStrategy = IterationStrategy.BULK_ITERATION;
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
    this.preprocessing = true;
    this.canonicalLabel = CanonicalLabel.MIN_DFS;
    this.filterStrategy = FilterStrategy.BROADCAST_JOIN;
    this.growthStrategy = GrowthStrategy.FUSION;
    this.iterationStrategy = IterationStrategy.BULK_ITERATION;
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
   * Getter for preprocessing flag.
   *
   * @return true, if preprocessing is enabled
   */
  public boolean usePreprocessing() {
    return preprocessing;
  }

  public CanonicalLabeler getCanonicalLabeler() {
    return canonicalLabel == CanonicalLabel.ADJACENCY_MATRIX ?
      new CAMLabeler(directed) : new MinDFSLabeler(directed);
  }

  public FilterStrategy getFilterStrategy() {
    return filterStrategy;
  }

  public GrowthStrategy getGrowthStrategy() {
    return growthStrategy;
  }

  public IterationStrategy getIterationStrategy() {
    return iterationStrategy;
  }

}
