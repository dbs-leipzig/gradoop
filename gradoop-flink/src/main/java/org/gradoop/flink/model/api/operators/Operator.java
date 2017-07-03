
package org.gradoop.flink.model.api.operators;

/**
 * Base interface for all EPGM operators.
 */
public interface Operator {
  /**
   * Returns the operators name.
   *
   * @return operator name
   */
  String getName();
}
