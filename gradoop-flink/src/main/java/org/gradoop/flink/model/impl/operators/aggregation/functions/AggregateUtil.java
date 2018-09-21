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

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.Map;
import java.util.Set;

/**
 * Utility functions for the aggregation operator
 */
class AggregateUtil {

  /**
   * Increments the aggregate map by the increment of the aggregate functions on the vertex
   *
   * @param aggregate aggregate map to be incremented
   * @param vertex vertex to increment with
   * @param aggregateFunctions aggregate functions
   * @return incremented aggregate map
   */
  static Map<String, PropertyValue> vertexIncrement(Map<String, PropertyValue> aggregate,
    Vertex vertex, Set<VertexAggregateFunction> aggregateFunctions) {
    for (VertexAggregateFunction aggFunc : aggregateFunctions) {
      PropertyValue increment = aggFunc.getVertexIncrement(vertex);
      if (increment != null) {
        aggregate.compute(aggFunc.getAggregatePropertyKey(), (key, agg) -> agg == null ?
          increment.copy() : aggFunc.aggregate(agg, increment));
      }
    }
    return aggregate;
  }

  /**
   * Increments the aggregate map by the increment of the aggregate functions on the edge
   *
   * @param aggregate aggregate map to be incremented
   * @param edge edge to increment with
   * @param aggregateFunctions aggregate functions
   * @return incremented aggregate map
   */
  static Map<String, PropertyValue> edgeIncrement(Map<String, PropertyValue> aggregate, Edge edge,
    Set<EdgeAggregateFunction> aggregateFunctions) {
    for (EdgeAggregateFunction aggFunc : aggregateFunctions) {
      PropertyValue increment = aggFunc.getEdgeIncrement(edge);
      if (increment != null) {
        aggregate.compute(aggFunc.getAggregatePropertyKey(), (key, agg) -> agg == null ?
          increment.copy() : aggFunc.aggregate(agg, increment));
      }
    }
    return aggregate;
  }
}
