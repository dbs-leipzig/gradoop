/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
    IN,
    OUT,
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
