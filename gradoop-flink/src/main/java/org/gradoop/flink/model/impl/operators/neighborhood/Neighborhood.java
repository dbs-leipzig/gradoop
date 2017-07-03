
package org.gradoop.flink.model.impl.operators.neighborhood;

import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Super class for all neighborhood operators.
 */
public abstract class Neighborhood implements UnaryGraphToGraphOperator {

  /**
   * Defines which edge direction shall be considered for aggregation. Incoming edges, outgoing
   * edges or both.
   */
  public enum EdgeDirection {
    /**
     * Incoming edge.
     */
    IN,
    /**
     * Outgoing edge.
     */
    OUT,
    /**
     * Incoming and outgoing edges.
     */
    BOTH
  }

  /**
   * Edge aggregate function.
   */
  private AggregateFunction function;

  /**
   * Considered edge direction for aggregation.
   */
  private EdgeDirection direction;

  /**
   * Valued constructor.
   *
   * @param function aggregate function
   * @param direction considered edge direction
   */
  public Neighborhood(AggregateFunction function, EdgeDirection direction) {
    this.function = function;
    this.direction = direction;
  }

  /**
   * Returns the aggregate function.
   *
   * @return aggregate function
   */
  public AggregateFunction getFunction() {
    return function;
  }

  /**
   * Returns the considered edge direction.
   *
   * @return edge direction
   */
  public EdgeDirection getDirection() {
    return direction;
  }
}
