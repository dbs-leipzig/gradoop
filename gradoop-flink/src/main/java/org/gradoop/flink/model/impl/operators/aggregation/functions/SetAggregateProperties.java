/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets aggregate values of a graph heads.
 */
public class SetAggregateProperties implements
  CoGroupFunction<GraphHead, Tuple2<GradoopId, Map<String, PropertyValue>>, GraphHead> {

  /**
   * default values used to replace aggregate values in case of NULL.
   */
  private final Map<String, PropertyValue> defaultValues;

  /**
   * Constructor.
   *
   * @param aggregateFunctions aggregate functions
   */
  public SetAggregateProperties(final Set<AggregateFunction> aggregateFunctions) {
    for (AggregateFunction func : aggregateFunctions) {
      checkNotNull(func);
    }

    defaultValues = new HashMap<>();

    for (AggregateFunction func : aggregateFunctions) {
      defaultValues.put(func.getAggregatePropertyKey(),
        func instanceof AggregateDefaultValue ?
          ((AggregateDefaultValue) func).getDefaultValue() :
          PropertyValue.NULL_VALUE);
    }
  }

  @Override
  public void coGroup(Iterable<GraphHead> left,
    Iterable<Tuple2<GradoopId, Map<String, PropertyValue>>> right, Collector<GraphHead> out) {

    for (GraphHead leftElem : left) {
      boolean rightEmpty = true;
      for (Tuple2<GradoopId, Map<String, PropertyValue>> rightElem : right) {
        rightElem.f1.forEach(leftElem::setProperty);
        out.collect(leftElem);
        rightEmpty = false;
      }
      if (rightEmpty) {
        defaultValues.forEach(leftElem::setProperty);
        out.collect(leftElem);
      }
    }
  }
}
