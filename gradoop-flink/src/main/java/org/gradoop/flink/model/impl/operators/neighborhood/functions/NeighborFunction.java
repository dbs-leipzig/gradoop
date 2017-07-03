
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.io.Serializable;

/**
 * Interface for all neighbor functions.
 */
public interface NeighborFunction extends Serializable {

  /**
   * Returns the aggregate function.
   *
   * @return aggregate function
   */
  AggregateFunction getFunction();
}
