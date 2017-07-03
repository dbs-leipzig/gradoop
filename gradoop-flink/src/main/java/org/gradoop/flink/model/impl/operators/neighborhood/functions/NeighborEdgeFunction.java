
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Super class for all neighbor edge functions.
 */
public abstract class NeighborEdgeFunction implements NeighborFunction {

  /**
   * Edge aggregation function.
   */
  private EdgeAggregateFunction function;

  /**
   * Valued constructor.
   *
   * @param function edge aggregation function
   */
  public NeighborEdgeFunction(EdgeAggregateFunction function) {
    this.function = function;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeAggregateFunction getFunction() {
    return function;
  }
}
