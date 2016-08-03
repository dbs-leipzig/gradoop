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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.math.BigDecimal;

/**
 * Reduces a dataset of property values, containing numeric elements,
 * to a dataset containing the sum of them as single element
 */
public class SumPropertyValuesGroups implements
  GroupReduceFunction<Tuple2<GradoopId, PropertyValue>, Tuple2<GradoopId,
    PropertyValue>> {
  /**
   * Instance of Number, containing a zero element of the same type as
   * the property values
   */
  private final Number zero;
  /**
   * Reduce object instantiation
   */
  private Tuple2<GradoopId, PropertyValue> reuseTuple;

  /**
   * Constructor
   *
   * @param zero zero element
   */
  public SumPropertyValuesGroups(Number zero) {
    this.reuseTuple = new Tuple2<>();
    this.zero = zero;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, PropertyValue>> in,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {
    Number result = zero;
    Class resultType = result.getClass();
    for (Tuple2<GradoopId, PropertyValue> tuple : in) {
      reuseTuple.f0 = tuple.f0;
      PropertyValue value = tuple.f1;
      // this is necessary to allow aggregation over a property that contains
      // values of different types (e.g. Integer and String)
      if (resultType == Integer.class && value.isInt()) {
        result = (Integer) result + value.getInt();
      } else if (resultType == Long.class && value.isLong()) {
        result = (Long) result + value.getLong();
      } else if (resultType == Float.class && value.isFloat()) {
        result = (Float) result + value.getFloat();
      } else if (resultType == Double.class && value.isDouble()) {
        result = (Double) result + value.getDouble();
      } else if (resultType == BigDecimal.class && value.isBigDecimal()) {
        result = ((BigDecimal) result).add(value.getBigDecimal());
      }
    }
    reuseTuple.f1 = PropertyValue.create(result);
    out.collect(reuseTuple);
  }
}
