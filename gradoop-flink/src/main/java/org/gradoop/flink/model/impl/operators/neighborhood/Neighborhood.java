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

public abstract class Neighborhood implements UnaryGraphToGraphOperator {

  public enum EdgeDirection {
    IN,
    OUT,
    BOTH
  }

  private AggregateFunction function;

  private EdgeDirection direction;

  Neighborhood(AggregateFunction function, EdgeDirection direction) {
    this.function = function;
    this.direction = direction;
  }

  public AggregateFunction getFunction() {
    return function;
  }

  public void setFunction(AggregateFunction function) {
    this.function = function;
  }

  public EdgeDirection getDirection() {
    return direction;
  }

  public void setDirection(EdgeDirection direction) {
    this.direction = direction;
  }
}
