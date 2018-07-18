/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
   * flag to enable preprocessingEnabled (true=enabled)
   */
  private final boolean preprocessingEnabled;

  /**
   * Constructor.
   *
   * @param minSupport min support
   * @param directed true, for directed mode
   * @param minEdgeCount min number of edges
   * @param maxEdgeCount max number of edges
   * @param preprocessingEnabled true, to enable preprocessingEnabled
   */
  public FSMConfig(
    float minSupport,
    boolean directed,
    int minEdgeCount,
    int maxEdgeCount,
    boolean preprocessingEnabled
  ) {
    this.minSupport = minSupport;
    this.directed = directed;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;
    this.preprocessingEnabled = preprocessingEnabled;
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
    this.preprocessingEnabled = true;
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
    this.preprocessingEnabled = true;
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
   * Getter for preprocessingEnabled flag.
   *
   * @return true, if preprocessingEnabled is enabled
   */
  public boolean isPreprocessingEnabled() {
    return preprocessingEnabled;
  }

}
