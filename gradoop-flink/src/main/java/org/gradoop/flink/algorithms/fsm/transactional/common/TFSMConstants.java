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

/**
 * Collection of broadcast dataset names used for frequent subgraph mining.
 */
public class TFSMConstants {

  /**
   * Cache counter name for the total number of graphs.
   */
  public static final String GRAPH_COUNT = "graphCount";

  /**
   * 1-element set with minimum frequency
   */
  public static final String MIN_FREQUENCY = "minFrequency";

  /**
   * frequent vertex labels
   */
  public static final String FREQUENT_VERTEX_LABELS = "fvl";

  /**
   * frequent edge labels
   */
  public static final String FREQUENT_EDGE_LABELS = "fel";

  /**
   * set of frequent patterns
   */
  public static final String FREQUENT_PATTERNS = "fp";

  /**
   * Graph head label of frequent subgraphs
   */
  public static final String FREQUENT_PATTERN_LABEL = "FrequentPattern";
  /**
   * Property key to store a frequent subgraphs's frequency.
   */
  public static final String SUPPORT_KEY = "frequency";
  /**
   * Property key to store the canonical label.
   */
  public static final String CANONICAL_LABEL_KEY = "canonicalLabel";
}
