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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Sets aggregate values of graph heads.
 *
 * @param <G> type of the graph head
 */
public class SetAggregateProperties<G extends GraphHead>
  implements CoGroupFunction<G, Tuple2<GradoopId, Map<String, PropertyValue>>, G> {

  /**
   * Aggregate functions from the aggregation step.
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * Creates a new instance of a SetAggregateProperties coGroup function.
   *
   * @param aggregateFunctions aggregate functions
   */
  public SetAggregateProperties(final Set<AggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = Objects.requireNonNull(aggregateFunctions);
  }

  @Override
  public void coGroup(Iterable<G> left, Iterable<Tuple2<GradoopId, Map<String, PropertyValue>>> right,
                      Collector<G> out) {

    for (G leftElem : left) {
      boolean rightEmpty = true;
      for (Tuple2<GradoopId, Map<String, PropertyValue>> rightElem : right) {
        Map<String, PropertyValue> values = rightElem.f1;
        // Apply post-aggregation step.
        for (AggregateFunction function : aggregateFunctions) {
          values.computeIfPresent(function.getAggregatePropertyKey(),
            (k, v) -> function.postAggregate(v));
          function.applyResult(leftElem, values.getOrDefault(function.getAggregatePropertyKey(),
            AggregateUtil.getDefaultAggregate(function)));
        }
        out.collect(leftElem);
        rightEmpty = false;
      }
      // For example if the graph is empty
      if (rightEmpty) {
        for (AggregateFunction function : aggregateFunctions) {
          function.applyResult(leftElem, AggregateUtil.getDefaultAggregate(function));
        }
        out.collect(leftElem);
      }
    }
  }
}
