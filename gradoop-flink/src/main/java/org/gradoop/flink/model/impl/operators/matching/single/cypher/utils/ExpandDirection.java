
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

/**
 * Specifies the direction of an expand operation
 */
public enum ExpandDirection {
  /**
   * Expand along incoming edges
   */
  IN,
  /**
   * Expand along outgoing edges
   */
  OUT
}
