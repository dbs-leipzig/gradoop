
package org.gradoop.flink.algorithms.fsm.dimspan.config;

/**
 * Collection of broadcast dataset names used for frequent subgraph mining.
 */
public class DIMSpanConstants {

  /**
   * Cardinality of input graph collection size.
   */
  public static final String GRAPH_COUNT = "|G|";

  /**
   * Minimum frequency derived from min support und graph count.
   */
  public static final String MIN_FREQUENCY = "fmin";

  /**
   * Vertex label dictionary.
   */
  public static final String VERTEX_DICTIONARY = "vld";

  /**
   * Edge label Dictionary
   */
  public static final String EDGE_DICTIONARY = "eld";

  /**
   * set of frequent patterns
   */
  public static final String FREQUENT_PATTERNS = "fp";

  /**
   * Graph head label of frequent patterns
   */
  public static final String FREQUENT_PATTERN_LABEL = "FrequentPattern";

  /**
   * Property key to store a frequent subgraphs's support.
   */
  public static final String SUPPORT_KEY = "support";

  /**
   * Property key to store the canonical label.
   */
  public static final String CANONICAL_LABEL_KEY = "minDFS";
}
