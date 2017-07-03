
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Creates a value from one input collection.
 *
 * @param <T> result type
 */
public interface UnaryGraphCollectionToValueOperator<T> {
  /**
   * Executes the operator.
   *
   * @param collection input collection
   * @return operator result
   */
  DataSet<T> execute(GraphCollection collection);
}
