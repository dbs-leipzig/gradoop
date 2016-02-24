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

package org.gradoop.model.impl.operators.aggregation.functions.max;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.impl.properties.PropertyValue;

import java.math.BigDecimal;

/**
 * Reduces a dataset of property values, containing numeric elements,
 * to a dataset containing the maximum of them as single element
 */
@FunctionAnnotation.ReadFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class MaxOfPropertyValues implements
  ReduceFunction<Tuple1<PropertyValue>> {

  /**
   * Instance of Number, containing a user defined maximum of the same type as
   * the property values
   */
  private final Number max;

  /**
   * Constructor
   * @param max maximum element
   */
  public MaxOfPropertyValues(Number max) {
    this.max = max;
  }
  @Override
  public Tuple1<PropertyValue> reduce(Tuple1<PropertyValue> prop1,
    Tuple1<PropertyValue> prop2) throws Exception {
    Class type = prop1.f0.getType();
    PropertyValue value1 = prop1.f0;
    PropertyValue value2 = prop2.f0;
    return new Tuple1<>(PropertyValue.create(type.equals(Integer.class) ?
      Math.max(value1.getInt(), value2.getInt()) :
        type.equals(Long.class) ?
      Math.max(value1.getLong(), value2.getLong()) :
        type.equals(Float.class) ?
      Math.max(value1.getFloat(), value2.getFloat()) :
        type.equals(Double.class) ?
      Math.max(value1.getDouble(), value2.getDouble()) :
        type.equals(BigDecimal.class) ?
      (value1.getBigDecimal().max(value2.getBigDecimal())) :
        max));
  }
}
