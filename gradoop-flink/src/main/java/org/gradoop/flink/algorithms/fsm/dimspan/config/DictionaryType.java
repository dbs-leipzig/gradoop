
package org.gradoop.flink.algorithms.fsm.dimspan.config;

/**
 * Dictionary coding options
 */
public enum DictionaryType implements Comparable<DictionaryType> {
  /**
   * No label pruning and alphabetical order.
   */
  RANDOM,
  /**
   * Higher label frequency <=> lower label (original gSpan)
   */
  INVERSE_PROPORTIONAL,
  /**
   * Higher label frequency <=> higher label
   */
  PROPORTIONAL,
  /**
   * label pruning but no alphabetical order.
   */
  FREQUENT
}
