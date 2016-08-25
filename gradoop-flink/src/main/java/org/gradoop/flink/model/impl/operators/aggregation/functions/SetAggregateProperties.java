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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets aggregate values of a graph heads.
 */
public class SetAggregateProperties implements
  CoGroupFunction<GraphHead, Tuple2<GradoopId, PropertyValue>, GraphHead> {

  /**
   * aggregate property key
   */
  private final String propertyKey;
  /**
   * default value used to replace aggregate value in case of NULL.
   */
  private final PropertyValue defaultValue;

  /**
   * Constructor.
   *
   * @param aggregateFunction aggregate function
   */
  public SetAggregateProperties(final AggregateFunction aggregateFunction) {
    checkNotNull(aggregateFunction);
    this.propertyKey = aggregateFunction.getAggregatePropertyKey();
    this.defaultValue = aggregateFunction instanceof AggregateDefaultValue ?
      ((AggregateDefaultValue) aggregateFunction).getDefaultValue() :
      PropertyValue.NULL_VALUE;
  }

  @Override
  public void coGroup(Iterable<GraphHead> left,
    Iterable<Tuple2<GradoopId, PropertyValue>> right, Collector<GraphHead> out
  ) throws Exception {

    for (GraphHead leftElem : left) {
      boolean rightEmpty = true;
      for (Tuple2<GradoopId, PropertyValue> rightElem : right) {
        leftElem.setProperty(propertyKey, rightElem.f1);
        out.collect(leftElem);
        rightEmpty = false;
      }
      if (rightEmpty) {
        leftElem.setProperty(propertyKey, defaultValue);
        out.collect(leftElem);
      }
    }
  }
}
