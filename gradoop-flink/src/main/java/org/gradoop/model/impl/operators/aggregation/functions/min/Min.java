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

package org.gradoop.model.impl.operators.aggregation.functions.min;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.functions.tuple.ValueOf1;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.functions.GetPropertyValue;
import org.gradoop.model.impl.operators.aggregation.functions
  .GraphIdsWithPropertyValue;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Utility method to compute the minimum of a property of elements in a dataset
 * without collecting it.
 */
public class Min {

  /**
   * Computes the minimum of the given property of elements in the given dataset
   * and stores the result in a 1-element dataset.
   *
   * @param dataSet input dataset
   * @param propertyKey key of property
   * @param max maximum of the same type as the property value
   * @param <EL>     element type in input dataset
   * @return 1-element dataset with minimum of input dataset
   */
  public static <EL extends EPGMElement> DataSet<PropertyValue> min(
    DataSet<EL> dataSet,
    String propertyKey,
    Number max) {
    return dataSet
      .map(new GetPropertyValue<EL>(propertyKey, max))
      .union(dataSet.getExecutionEnvironment()
        .fromElements(new Tuple1<>(PropertyValue.create(max))))
      .reduce(new MinOfPropertyValues(max))
      .map(new ValueOf1<PropertyValue>());
  }

  /**
   * Groups the input dataset by the contained elements and computes the minimum
   * of a property for each group.
   * Returns a {@code Tuple2} containing the group element and the
   * corresponding minimum value.
   *
   * @param dataSet input dataset
   * @param propertyKey key of property
   * @param max maximum, of the same type as the property value
   * @param <EL>     element type in input dataset
   * @return {@code Tuple2} with group value and group minimum
   */
  public static <EL extends EPGMGraphElement>
  DataSet<Tuple2<GradoopId, PropertyValue>> groupBy(
    DataSet<EL> dataSet,
    String propertyKey,
    Number max) {
    return dataSet
      .flatMap(new GraphIdsWithPropertyValue<EL>(propertyKey))
      .groupBy(0)
      .reduceGroup(new MinOfPropertyValuesGroups(max));
  }
}
