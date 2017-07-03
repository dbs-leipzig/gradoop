
package org.gradoop.flink.model.api.operators;

/**
 * A marker interface for instances of {@link BinaryGraphToGraphOperator} that
 * support the reduction of a collection to a single logical graph.
 */
public interface ReducibleBinaryGraphToGraphOperator
  extends UnaryCollectionToGraphOperator {
}
