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
/**
 * edge,.. => aggregateValue
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function.
 */
public class AggregateEdges
  implements GroupCombineFunction<Edge, PropertyValue> {

  /**
   * Aggregate function.
   */
  private final EdgeAggregateFunction aggFunc;

  /**
   * Constructor.
   *
   * @param aggFunc aggregate function
   */
  public AggregateEdges(EdgeAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(
    Iterable<Edge> edges, Collector<PropertyValue> out) throws Exception {
    PropertyValue aggregate = null;

    for (Edge edge : edges) {
      PropertyValue increment = aggFunc.getEdgeIncrement(edge);
      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggFunc.aggregate(aggregate, increment);
        }
      }
    }

    if (aggregate != null) {
      out.collect(aggregate);
    }
  }
}
