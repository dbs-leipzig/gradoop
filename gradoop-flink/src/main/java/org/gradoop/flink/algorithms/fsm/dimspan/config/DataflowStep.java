
package org.gradoop.flink.algorithms.fsm.dimspan.config;

/**
 * Options to execute single operation at different positions of the dataflow.
 */
public enum DataflowStep implements Comparable<DataflowStep> {
  /**
   * After pattern-growth map function.
   */
  MAP,
  /**
   * After local pattern frequency counting.
   */
  COMBINE,
  /**
   * After pattern frequency pruning.
   */
  FILTER,
  /**
   * Never.
   */
  WITHOUT
}
