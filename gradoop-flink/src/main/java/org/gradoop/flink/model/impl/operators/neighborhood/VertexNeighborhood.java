
package org.gradoop.flink.model.impl.operators.neighborhood;

import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Super class for all vertex neighborhood operators.
 */
public abstract class VertexNeighborhood extends Neighborhood {

  /**
   * Valued constructor.
   *
   * @param function  vertex aggregate function
   * @param direction considered edge direction
   */
  VertexNeighborhood(VertexAggregateFunction function, EdgeDirection direction) {
    super(function, direction);
  }
}
