/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Applies aggregate functions to the elements.
 *
 * @param <T> element type
 */
public class AggregateElements<T extends Element>
  implements GroupCombineFunction<T, Map<String, PropertyValue>> {

  /**
   * Aggregate functions.
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * Creates a new instance of a AggregateElements group combine function.
   *
   * @param aggregateFunctions aggregate functions
   */
  public AggregateElements(Set<AggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void combine(Iterable<T> elements, Collector<Map<String, PropertyValue>> out) {
    Map<String, PropertyValue> aggregate = new HashMap<>();

    for (T element : elements) {
      aggregate = AggregateUtil.increment(aggregate, element, aggregateFunctions);
    }

    if (!aggregate.isEmpty()) {
      out.collect(aggregate);
    }
  }
}
