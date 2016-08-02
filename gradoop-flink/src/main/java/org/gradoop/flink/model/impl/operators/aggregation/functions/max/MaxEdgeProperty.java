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

package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateWithDefaultValueFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Aggregate function returning the maximum of a specified property over all
 * edges.
 */
public class MaxEdgeProperty extends AggregateWithDefaultValueFunction {

  /**
   * Property key to retrieve property values
   */
  private String propertyKey;

  /**
   * Constructor
   * @param propertyKey Property key to retrieve property values
   * @param min user defined minimum, used as default property value
   */
  public MaxEdgeProperty(
    String propertyKey,
    Number min) {
    super(min);
    this.propertyKey = propertyKey;
  }

  /**
   * Returns a 1-element dataset containing the maximum of the given property
   * over the given elements.
   *
   * @param graph input graph
   * @return 1-element dataset with vertex count
   */
  @Override
  public DataSet<PropertyValue> execute(LogicalGraph graph) {
    return Max.max(graph.getEdges(),
      propertyKey,
      getDefaultValue());
  }

  /**
   * Returns a dataset containing graph identifiers and the corresponding edge
   * maximum.
   *
   * @param collection input graph collection
   * @return dataset with graph + edge count tuples
   */
  @Override
  public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
    GraphCollection collection) {
    return Max.groupBy(collection.getEdges(),
      propertyKey,
      getDefaultValue());
  }
}
