/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
