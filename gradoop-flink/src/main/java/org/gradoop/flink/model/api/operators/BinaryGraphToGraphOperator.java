
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.combination.Combination;

/**
 * Creates a {@link LogicalGraph} based on two input graphs.
 *
 * @see Combination
 * @see Exclusion
 * @see Overlap
 */
public interface BinaryGraphToGraphOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstGraph  first input graph
   * @param secondGraph second input graph
   * @return operator result
   */
  LogicalGraph execute(LogicalGraph firstGraph, LogicalGraph secondGraph);
}
