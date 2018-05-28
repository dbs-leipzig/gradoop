/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
