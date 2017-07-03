
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.operators.union.Union;

/**
 * Creates a {@link GraphCollection} based on two input collections.
 *
 * @see Union
 * @see Intersection
 * @see Difference
 */
public interface BinaryCollectionToCollectionOperator extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstCollection  first input collection
   * @param secondCollection second input collection
   * @return operator result
   */
  GraphCollection execute(GraphCollection firstCollection,
    GraphCollection secondCollection);
}
