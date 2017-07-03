
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Creates a {@link GraphCollection} based on one input collection.
 *
 */
public interface UnaryCollectionToCollectionOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param collection input collection
   * @return operator result
   */
  GraphCollection execute(GraphCollection collection);
}
