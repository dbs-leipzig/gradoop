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

/**
 * Collection of broadcast dataset names used for frequent subgraph mining.
 */
public class BroadcastNames {

  /**
   * set of frequent subgraphs
   */
  public static final String FREQUENT_SUBGRAPHS = "frequentSubgraphs";

  /**
   * 1-element set with minimum frequency
   */
  public static final String MIN_FREQUENCY = "minFrequency";

  /**
   * vertex label dictionary
   */
  public static final String VERTEX_DICTIONARY = "vertexLabelDictionary";

  /**
   * vertex label dictionary
   */
  public static final String EDGE_DICTIONARY = "edgeLabelDictionary";

  /**
   * graph counts of all workers
   */
  public static final String WORKER_GRAPHCOUNT = "workerIds";
}
