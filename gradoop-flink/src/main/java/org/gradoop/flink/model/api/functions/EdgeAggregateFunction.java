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

package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes an edge aggregate function as input for the
 * {@link Aggregation} operator.
 */
public interface EdgeAggregateFunction extends AggregateFunction {

  /**
   * Describes the increment of an edge that should be added to the aggregate.
   *
   * @param edge edge
   *
   * @return increment, may be NULL, which is handled in the operator
   */
  PropertyValue getEdgeIncrement(Edge edge);
}
