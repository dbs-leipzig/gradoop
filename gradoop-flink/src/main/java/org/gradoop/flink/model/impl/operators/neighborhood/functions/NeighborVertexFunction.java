
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Super class for all neighbor vertex functions.
 */
public class NeighborVertexFunction implements NeighborFunction {

  /**
   * Vertex aggregation function.
   */
  private VertexAggregateFunction function;

  /**
   * Valued constructor.
   *
   * @param function vertex aggregation function
   */
  public NeighborVertexFunction(VertexAggregateFunction function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexAggregateFunction getFunction() {
    return function;
  }
}
