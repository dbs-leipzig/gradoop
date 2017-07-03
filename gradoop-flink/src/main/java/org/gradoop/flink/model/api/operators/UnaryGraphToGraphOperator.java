
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Creates a {@link LogicalGraph} based on one input {@link LogicalGraph}.
 */
public interface UnaryGraphToGraphOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  LogicalGraph execute(LogicalGraph graph);
}
