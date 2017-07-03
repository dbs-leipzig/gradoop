
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Creates a {@link GraphCollection} based on one {@link LogicalGraph}.
 */
public interface UnaryGraphToCollectionOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param graph input graph
   * @return operator result
   */
  GraphCollection execute(LogicalGraph graph);
}
