
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Creates a {@link LogicalGraph} from one input collection.
 */
public interface UnaryCollectionToGraphOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param collection input collection
   * @return operator result
   */
  LogicalGraph execute(GraphCollection collection);
}
