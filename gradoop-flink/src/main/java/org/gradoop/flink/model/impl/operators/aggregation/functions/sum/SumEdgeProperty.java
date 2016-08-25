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

package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function returning the sum of a specified property over all edges.
 */
public class SumEdgeProperty extends SumProperty implements EdgeAggregateFunction {

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public SumEdgeProperty(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return edge.getPropertyValue(propertyKey);
  }

}
