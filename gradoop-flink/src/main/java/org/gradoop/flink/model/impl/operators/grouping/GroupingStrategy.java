
package org.gradoop.flink.model.impl.operators.grouping;

/**
 * Used to define the grouping strategy which is used for computing the summary
 * graph.
 */
public enum GroupingStrategy {
  /**
   * {@see GroupingGroupReduce}
   */
  GROUP_REDUCE,
  /**
   * {@see GroupingGroupCombine}
   */
  GROUP_COMBINE
}
