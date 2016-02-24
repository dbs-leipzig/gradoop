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

package org.gradoop.model.impl.operators.aggregation.functions.sum;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

import java.math.BigDecimal;

/**
 * Reduces a dataset of property values, containing numeric elements,
 * to a dataset containing the sum of them as single element
 */
public class SumPropertyValuesGroups implements
  GroupReduceFunction<Tuple2<GradoopId, PropertyValue>, Tuple2<GradoopId,
    PropertyValue>> {

  /**
   * Instance of Number, containing 0 of the same type as
   * the property values
   */
  private final Number zero;

  /**
   * Constructor
   * @param zero zero element
   */
  public SumPropertyValuesGroups(Number zero) {
    this.zero = zero;
  }

  @Override
  public void reduce(
    Iterable<Tuple2<GradoopId, PropertyValue>> in,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {
    Number result = zero;
    GradoopId id = GradoopId.get();
    Class type = result.getClass();
    for (Tuple2<GradoopId, PropertyValue> tuple : in) {
      id = tuple.f0;
      PropertyValue value = tuple.f1;
      result =
        type.equals(Integer.class) ?
          (Integer) result + value.getInt() :
        type.equals(Long.class) ?
          (Long) result + value.getInt() :
        type.equals(Float.class) ?
          (Float) result + value.getFloat() :
        type.equals(Double.class) ?
          (Double) result + value.getDouble() :
        type.equals(BigDecimal.class) ?
          ((BigDecimal) result).add(value.getBigDecimal()) :
        result;
    }
    out.collect(new Tuple2<>(
      id,
      PropertyValue.create(result)
    ));
  }
}
