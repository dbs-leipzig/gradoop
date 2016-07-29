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

package org.gradoop.model.api.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Describes an aggregate function as input for the
 * {@link org.gradoop.model.impl.operators.aggregation.Aggregation} operator.
 */
public interface AggregateFunction {

  /**
   * Defines the aggregate function.
   *
   * @param graph input graph
   * @return aggregated value as 1-element dataset
   */
  DataSet<PropertyValue> execute(LogicalGraph graph);

}
