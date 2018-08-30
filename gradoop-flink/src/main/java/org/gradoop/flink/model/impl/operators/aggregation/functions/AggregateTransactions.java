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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
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
   * Set of aggregate vertex functions.
   */
  private final Set<VertexAggregateFunction> vertexAggregateFunctions;
  /**
   * Set of aggregate edge functions.
   */
  private final Set<EdgeAggregateFunction> edgeAggregateFunctions;
  /**
   * Set of aggregate default values.
   */
  private final Map<String, PropertyValue> aggregateDefaultValues;

  /**
   * Creates a new instance.
   *
   * @param aggregateFunctions vertex or edge aggregate functions with possible default value
   */
  public AggregateTransactions(Set<AggregateFunction> aggregateFunctions) {
    // initialization logic to avoid instanceOf checking during execution

    vertexAggregateFunctions = aggregateFunctions.stream()
      .filter(f -> f instanceof VertexAggregateFunction)
      .map(VertexAggregateFunction.class::cast)
      .collect(Collectors.toSet());

    edgeAggregateFunctions = aggregateFunctions.stream()
      .filter(f -> f instanceof EdgeAggregateFunction)
      .map(EdgeAggregateFunction.class::cast)
      .collect(Collectors.toSet());

    aggregateDefaultValues = new HashMap<>();
    for (AggregateFunction func : aggregateFunctions) {
      aggregateDefaultValues.put(func.getAggregatePropertyKey(),
        func instanceof AggregateDefaultValue ?
          ((AggregateDefaultValue) func).getDefaultValue() :
          PropertyValue.NULL_VALUE);
    }
  }

  @Override
  public GraphTransaction map(GraphTransaction graphTransaction) throws Exception {

    Map<String, PropertyValue> aggregate = aggregateVertices(graphTransaction);
    aggregate.putAll(aggregateEdges(graphTransaction));

    aggregateDefaultValues.forEach(aggregate::putIfAbsent);

    aggregate.forEach(graphTransaction.getGraphHead()::setProperty);
    return graphTransaction;
  }

  /**
   * Applies the aggregate functions on the vertices of the given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return final vertex aggregate value
   */
  private Map<String, PropertyValue> aggregateVertices(GraphTransaction graphTransaction) {
    Map<String, PropertyValue> aggregate = new HashMap<>();

    for (Vertex vertex : graphTransaction.getVertices()) {
      aggregate = AggregateUtil.vertexIncrement(aggregate, vertex, vertexAggregateFunctions);
    }
    return aggregate;
  }

  /**
   * Applies the aggregate functions on the edges of the given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return final edge aggregate value
   */
  private Map<String, PropertyValue> aggregateEdges(GraphTransaction graphTransaction) {
    Map<String, PropertyValue> aggregate = new HashMap<>();

    for (Edge edge : graphTransaction.getEdges()) {
      aggregate = AggregateUtil.edgeIncrement(aggregate, edge, edgeAggregateFunctions);
    }
    return aggregate;
  }
}
