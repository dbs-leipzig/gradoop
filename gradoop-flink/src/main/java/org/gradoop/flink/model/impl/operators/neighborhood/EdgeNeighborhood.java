
package org.gradoop.flink.model.impl.operators.neighborhood;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Super class for all edge neighborhood operators.
 */
public abstract class EdgeNeighborhood extends Neighborhood {

  /**
   * Valued constructor.
   *
   * @param function  edge aggregate function
   * @param direction considered edge direction
   */
  public EdgeNeighborhood(EdgeAggregateFunction function, EdgeDirection direction) {
    super(function, direction);
  }
}
