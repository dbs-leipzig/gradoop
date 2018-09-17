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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets aggregate values of a graph head.
 */
@FunctionAnnotation.ForwardedFields("id")
public class SetAggregateProperty
  extends RichMapFunction<GraphHead, GraphHead> {

  /**
   * constant string for accessing broadcast variable "property values"
   */
  public static final String VALUE = "value";

  /**
   * map from aggregate property key to its value
   */
  private Map<String, PropertyValue> aggregateValues;

  /**
   * map from aggregate property key to its default value
   * used to replace aggregate value in case of NULL.
   */
  private final Map<String, PropertyValue> defaultValues;


  /**
   * Constructor.
   *
   * @param aggregateFunctions aggregate functions
   */
  public SetAggregateProperty(Set<AggregateFunction> aggregateFunctions) {
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

  @SuppressWarnings("unchecked")
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    if (getRuntimeContext().getBroadcastVariable(VALUE).isEmpty()) {
      aggregateValues = defaultValues;
    } else {
      aggregateValues = (Map<String, PropertyValue>) getRuntimeContext()
        .getBroadcastVariable(VALUE).get(0);
      defaultValues.forEach(aggregateValues::putIfAbsent);
    }
  }

  @Override
  public GraphHead map(GraphHead graphHead) throws Exception {
    aggregateValues.forEach(graphHead::setProperty);
    return graphHead;
  }
}
