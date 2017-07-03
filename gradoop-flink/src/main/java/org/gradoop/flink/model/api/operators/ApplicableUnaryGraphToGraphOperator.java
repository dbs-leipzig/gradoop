
package org.gradoop.flink.model.api.operators;

/**
 * A marker interface for instances of {@link UnaryGraphToGraphOperator} that
 * support the application on each element in a graph collection.
 */
public interface ApplicableUnaryGraphToGraphOperator
  extends UnaryCollectionToCollectionOperator {
}
