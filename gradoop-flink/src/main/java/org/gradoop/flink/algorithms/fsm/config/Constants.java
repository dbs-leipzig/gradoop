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
public class Constants {

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
  public static final String FREQUENT_VERTEX_LABELS = "vertexLabelDictionary";

  /**
   * vertex label dictionary
   */
  public static final String FREQUENT_EDGE_LABELS = "edgeLabelDictionary";

  /**
   * graph counts of all workers
   */
  public static final String WORKER_GRAPH_COUNT = "workerIds";

  /**
   * Cache event name for finished tasks.
   */
  public static final String TASK_FINISHED = "tf";

  /**
   * Vertex prefix for cache objects.
   */
  public static final String VERTEX_PREFIX = "v";

  /**
   * Edge prefix for cache objects.
   */
  public static final String EDGE_PREFIX = "e";

  /**
   * Cache map name for a label dictionary.
   */
  public static final String LABEL_DICTIONARY = "ld";

  /**
   * Cache list name for an inverse label dictionary.
   */
  public static final String LABEL_DICTIONARY_INVERSE = "ldi";

  /**
   * Cache event name for an available label dictionary.
   */
  public static final String LABEL_DICTIONARY_AVAILABLE = "lda";

  /**
   * Cache counter name for the total number of graphs.
   */
  public static final String GRAPH_COUNT = "gc";

  /**
   * Cache counter name for the total number of partitions reported their
   * graph count.
   */
  public static final String GRAPH_COUNT_REPORTS = "gcr";
}
