
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Creates a (usually 1-element) Boolean dataset based on two input graphs.
 *
 * @param <T> value type
 */
public interface BinaryCollectionToValueOperator<T> extends Operator {
  /**
   * Executes the operator.
   *
   * @param firstCollection  first input collection
   * @param secondCollection second input collection
   * @return operator result
   */
  DataSet<T> execute(GraphCollection firstCollection,
    GraphCollection secondCollection);
}
