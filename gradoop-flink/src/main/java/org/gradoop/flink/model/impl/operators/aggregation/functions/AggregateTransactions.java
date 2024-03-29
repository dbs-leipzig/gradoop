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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies an {@link AggregateFunction} to the vertex or edge set of a graph transaction.
 */
public class AggregateTransactions implements MapFunction<GraphTransaction, GraphTransaction> {

  /**
   * Set of all aggregate functions.
   */
  private final Set<AggregateFunction> aggregateFunctions;
  /**
   * Set of aggregate vertex functions.
   */
  private final Set<AggregateFunction> vertexAggregateFunctions;
  /**
   * Set of aggregate edge functions.
   */
  private final Set<AggregateFunction> edgeAggregateFunctions;

  /**
   * Creates a new instance of a AggregateTransactions map function.
   *
   * @param aggregateFunctions vertex or edge aggregate functions with possible default value
   */
  public AggregateTransactions(Set<AggregateFunction> aggregateFunctions) {
    // initialization logic to avoid instanceOf checking during execution
    this.aggregateFunctions = aggregateFunctions;

    vertexAggregateFunctions = aggregateFunctions.stream()
      .filter(AggregateFunction::isVertexAggregation)
      .collect(Collectors.toSet());

    edgeAggregateFunctions = aggregateFunctions.stream()
      .filter(AggregateFunction::isEdgeAggregation)
      .collect(Collectors.toSet());
  }

  @Override
  public GraphTransaction map(GraphTransaction graphTransaction) throws Exception {
    Map<String, PropertyValue> aggregate = new HashMap<>();
    aggregate = aggregateVertices(aggregate, graphTransaction);
    aggregate = aggregateEdges(aggregate, graphTransaction);

    for (AggregateFunction function : aggregateFunctions) {
      aggregate.computeIfPresent(function.getAggregatePropertyKey(),
        (k, v) -> function.postAggregate(v));
      function.applyResult(graphTransaction.getGraphHead(), aggregate
        .getOrDefault(function.getAggregatePropertyKey(), AggregateUtil.getDefaultAggregate(function)));
    }
    return graphTransaction;
  }

  /**
   * Applies the aggregate functions on the vertices of the given graph transaction.
   *
   * @param aggregate map to hold the aggregate values
   * @param graphTransaction graph transaction
   * @return final vertex aggregate value
   */
  private Map<String, PropertyValue> aggregateVertices(Map<String, PropertyValue> aggregate,
                                                       GraphTransaction graphTransaction) {
    for (EPGMVertex vertex : graphTransaction.getVertices()) {
      aggregate = AggregateUtil.increment(aggregate, vertex, vertexAggregateFunctions);
    }
    return aggregate;
  }

  /**
   * Applies the aggregate functions on the edges of the given graph transaction.
   *
   * @param aggregate map to hold the aggregate values
   * @param graphTransaction graph transaction
   * @return final edge aggregate value
   */
  private Map<String, PropertyValue> aggregateEdges(Map<String, PropertyValue> aggregate,
                                                    GraphTransaction graphTransaction) {
    for (EPGMEdge edge : graphTransaction.getEdges()) {
      aggregate = AggregateUtil.increment(aggregate, edge, edgeAggregateFunctions);
    }
    return aggregate;
  }
}
