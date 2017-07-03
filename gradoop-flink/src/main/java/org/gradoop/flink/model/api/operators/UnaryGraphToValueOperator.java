
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Creates a value from one input {@link org.gradoop.flink.model.impl.LogicalGraph}.
 *
 * @param <T> result type
 */
public interface UnaryGraphToValueOperator<T> {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  T execute(LogicalGraph graph);
}
