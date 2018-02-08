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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Applies an {@link AggregateFunction} to the vertex or edge set of a graph transaction.
 */
public class AggregateTransactions implements MapFunction<GraphTransaction, GraphTransaction> {

  /**
   * True, if the given aggregate function is a {@link VertexAggregateFunction}.
   * False, if the given aggregate function is a {@link EdgeAggregateFunction}.
   */
  private final boolean isVertexAggregateFunction;
  /**
   * True, iff the given aggregate function has a default value.
   */
  private final boolean hasAggregateDefaultValue;
  /**
   * Property key used to store the final aggregate at the graph head of the graph transaction.
   */
  private final String aggregatePropertyKey;
  /**
   * Set to an instance of {@link VertexAggregateFunction} if
   * {@link AggregateTransactions#isVertexAggregateFunction} is {@code true}.
   */
  private final VertexAggregateFunction vertexAggregateFunction;
  /**
   * Set to an instance of {@link EdgeAggregateFunction} if
   * {@link AggregateTransactions#isVertexAggregateFunction} is {@code false}.
   */
  private final EdgeAggregateFunction edgeAggregateFunction;
  /**
   * Set to an instance of {@link AggregateDefaultValue} if
   * {@link AggregateTransactions#hasAggregateDefaultValue} is {@code true}.
   */
  private final AggregateDefaultValue aggregateDefaultValue;

  /**
   * Creates a new instance.
   *
   * @param aggregateFunction vertex or edge aggregate function with possible default value
   */
  public AggregateTransactions(AggregateFunction aggregateFunction) {
    // initialization logic to avoid instanceOf checking during execution
    aggregatePropertyKey = aggregateFunction.getAggregatePropertyKey();

    if (aggregateFunction instanceof VertexAggregateFunction) {
      isVertexAggregateFunction = true;
      vertexAggregateFunction = (VertexAggregateFunction) aggregateFunction;
      edgeAggregateFunction = null;
    } else {
      isVertexAggregateFunction = false;
      vertexAggregateFunction = null;
      edgeAggregateFunction = (EdgeAggregateFunction) aggregateFunction;
    }

    if (aggregateFunction instanceof AggregateDefaultValue) {
      hasAggregateDefaultValue = true;
      aggregateDefaultValue = (AggregateDefaultValue) aggregateFunction;
    } else {
      hasAggregateDefaultValue = false;
      aggregateDefaultValue = null;
    }
  }

  @Override
  public GraphTransaction map(GraphTransaction graphTransaction) throws Exception {

    PropertyValue aggregate = isVertexAggregateFunction ?
      aggregateVertices(graphTransaction) :
      aggregateEdges(graphTransaction);

    if (aggregate == null) {
      if (hasAggregateDefaultValue) {
        aggregate = aggregateDefaultValue.getDefaultValue();
      } else {
        aggregate = PropertyValue.NULL_VALUE;
      }
    }

    graphTransaction.getGraphHead().setProperty(aggregatePropertyKey, aggregate);

    return graphTransaction;
  }

  /**
   * Applies the aggregate function on the vertices of the given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return final aggregate value or {@code null} if no vertices are contained
   */
  private PropertyValue aggregateVertices(GraphTransaction graphTransaction) {
    return iterate(graphTransaction.getVertices().iterator(),
      vertexAggregateFunction::getVertexIncrement,
      vertexAggregateFunction::aggregate);
  }

  /**
   * Applies the aggregate function on the edges of the given graph transaction.
   *
   * @param graphTransaction graph transaction
   * @return final aggregate value or {@code null} if no edges are contained
   */
  private PropertyValue aggregateEdges(GraphTransaction graphTransaction) {
    return iterate(graphTransaction.getEdges().iterator(),
      edgeAggregateFunction::getEdgeIncrement,
      edgeAggregateFunction::aggregate);
  }

  /**
   * Iterates the given set of EPGM elements and computes the aggregate value given the two
   * functions.
   *
   * @param elements iterator of EPGM elements
   * @param valueFunction extracts the property value used for aggregation
   * @param aggregateFunction used to update the aggregate
   * @param <T> EPGM element type
   * @return final aggregate value or {@code null} if {@code elements} is empty
   */
  private <T extends Element> PropertyValue iterate(Iterator<T> elements,
    Function<T, PropertyValue> valueFunction,
    BiFunction<PropertyValue, PropertyValue, PropertyValue> aggregateFunction) {

    PropertyValue aggregate = null;

    while (elements.hasNext()) {
      PropertyValue increment = valueFunction.apply(elements.next());
      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggregateFunction.apply(aggregate, increment);
        }
      }
    }
    return aggregate;
  }
}
