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

/**
 * Reduces a dataset of property values, containing numeric elements,
 * to a dataset containing the maximum of them as single element
 */
@FunctionAnnotation.ReadFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class MaxOfPropertyValues implements
  ReduceFunction<Tuple1<PropertyValue>> {
  /**
   * Instance of Number, containing a minimum of the same type as
   * the property values
   */
  private final Number min;
  /**
   * Reduce object instantiation
   */
  private Tuple1<PropertyValue> reuseTuple;

  /**
   * Constructor
   *
   * @param min minimum element
   */
  public MaxOfPropertyValues(Number min) {
    this.reuseTuple = new Tuple1<>();
    this.min = min;
  }

  @Override
  public Tuple1<PropertyValue> reduce(Tuple1<PropertyValue> prop1,
    Tuple1<PropertyValue> prop2) throws Exception {
    PropertyValue value1 = prop1.f0;
    PropertyValue value2 = prop2.f0;
    // this is necessary to allow aggregation over a property that contains
    // values of different types (e.g. Integer and String)
    if (value1.isInt() && value2.isInt()) {
      reuseTuple.f0 =
        PropertyValue.create(Math.max(value1.getInt(), value2.getInt()));
    } else if (value1.isLong() && value2.isLong()) {
      reuseTuple.f0 =
        PropertyValue.create(Math.max(value1.getLong(), value2.getLong()));
    } else if (value1.isFloat() && value2.isFloat()) {
      reuseTuple.f0 =
        PropertyValue.create(Math.max(value1.getFloat(), value2.getFloat()));
    } else if (value1.isDouble() && value2.isDouble()) {
      reuseTuple.f0 =
        PropertyValue.create(Math.max(value1.getDouble(), value2.getDouble()));
    } else if (value1.isBigDecimal() && value2.isBigDecimal()) {
      reuseTuple.f0 = PropertyValue
        .create(value1.getBigDecimal().max(value2.getBigDecimal()));
    } else {
      reuseTuple.f0 = PropertyValue.create(min);
    }
    return reuseTuple;
  }
}
