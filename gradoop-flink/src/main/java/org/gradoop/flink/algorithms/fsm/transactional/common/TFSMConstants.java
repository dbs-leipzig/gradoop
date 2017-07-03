
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
