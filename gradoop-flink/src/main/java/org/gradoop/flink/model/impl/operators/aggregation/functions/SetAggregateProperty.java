/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Sets aggregate values of a graph head.
 *
 * @param <G> The graph head type.
 */
@FunctionAnnotation.ForwardedFields("id")
public class SetAggregateProperty<G extends GraphHead>
  extends RichMapFunction<G, G> {

  /**
   * constant string for accessing broadcast variable "property values"
   */
  public static final String VALUE = "value";

  /**
   * The set of used aggregate functions.
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * map from aggregate property key to its value
   */
  private Map<String, PropertyValue> aggregateValues;


  /**
   * Creates a new instance of a SetAggregateProperty rich map function.
   *
   * @param aggregateFunctions aggregate functions
   */
  public SetAggregateProperty(Set<AggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = Objects.requireNonNull(aggregateFunctions);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    if (getRuntimeContext().getBroadcastVariable(VALUE).isEmpty()) {
      aggregateValues = new HashMap<>();
    } else {
      aggregateValues = (Map<String, PropertyValue>) getRuntimeContext()
        .getBroadcastVariable(VALUE).get(0);
      // Compute post-aggregate functions.
      for (AggregateFunction function : aggregateFunctions) {
        aggregateValues.computeIfPresent(function.getAggregatePropertyKey(),
          (k, v) -> function.postAggregate(v));
      }
    }
  }

  @Override
  public G map(G graphHead) throws Exception {
    aggregateFunctions.forEach(f -> f.applyResult(graphHead,
      aggregateValues.getOrDefault(f.getAggregatePropertyKey(), AggregateUtil.getDefaultAggregate(f))));
    return graphHead;
  }
}
