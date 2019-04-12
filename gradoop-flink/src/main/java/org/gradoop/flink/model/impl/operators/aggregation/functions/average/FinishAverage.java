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
package org.gradoop.flink.model.impl.operators.aggregation.functions.average;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;

import java.util.List;
import java.util.Objects;

/**
 * Finish an average aggregation by calculating the actual average from the internally
 * used values.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The graph type.
 * @see AverageProperty the description of the internally used aggregate value.
 */
public class FinishAverage<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>> implements UnaryBaseGraphToBaseGraphOperator<LG>,
  MapFunction<G, G> {

  /**
   * The property key used to read the internal value and write the average.
   */
  private final String propertyKey;

  /**
   * Create an instance of this function.
   *
   * @param propertyKey The key used to read and write the values. (The same property key used
   *                    by {@link AverageProperty}.)
   */
  public FinishAverage(String propertyKey) {
    this.propertyKey = Objects.requireNonNull(propertyKey);
  }

  @Override
  public LG execute(LG graph) {
    return graph.getFactory().fromDataSets(graph.getGraphHead().map(this),
      graph.getVertices(), graph.getEdges());
  }

  @Override
  public G map(G head) {
    List<PropertyValue> internalResult = AverageProperty.validateAndGetValue(
      head.getPropertyValue(propertyKey));
    double sum = internalResult.get(0).getDouble();
    double count = internalResult.get(1).getDouble();
    if (count < 0) {
      throw new IllegalArgumentException("Invalid number of elements " + count + ", expected " +
        "value greater than zero.");
    } else if (count == 0) {
      head.setProperty(propertyKey, PropertyValue.NULL_VALUE);
    } else {
      head.setProperty(propertyKey, PropertyValue.create(sum / count));
    }
    return head;
  }
}
