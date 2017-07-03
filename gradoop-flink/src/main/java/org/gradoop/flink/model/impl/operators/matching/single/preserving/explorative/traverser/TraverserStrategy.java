
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser;

/**
 * Defines the strategy to traverse the graph.
 */
public enum TraverserStrategy {
  /**
   * Traverse the graph using bulk iteration.
   */
  SET_PAIR_BULK_ITERATION,
  /**
   * Traverse the graph using a for loop iteration.
   */
  SET_PAIR_FOR_LOOP_ITERATION,
  /**
   * Traverse the graph based on edge triples in a for loop.
   */
  TRIPLES_FOR_LOOP_ITERATION
}
