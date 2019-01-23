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
/**
 * (graphId,vertex),.. => (graphId,aggregateValue),..
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Applies vertex aggregate functions to vertices with the same graph id.
 * (graphId,vertex),.. => (graphId,[aggregateKey,aggregateValue]),..
 *
 * @param <T> element type
 */
public class ApplyAggregateElements<T extends Element> implements GroupCombineFunction
  <Tuple2<GradoopId, T>, Tuple2<GradoopId, Map<String, PropertyValue>>> {

  /**
   * Aggregate functions.
   */
  private final Set<AggregateFunction> aggregateFunctions;
  /**
   * Reuse tuple.
   */
  private final Tuple2<GradoopId, Map<String, PropertyValue>> reusePair = new Tuple2<>();

  /**
   * Creates a new instance of a ApplyAggregateElements group combine function.
   *
   * @param aggregateFunctions aggregate functions
   */
  public ApplyAggregateElements(Set<AggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, T>> elements,
    Collector<Tuple2<GradoopId, Map<String, PropertyValue>>> out) {

    Iterator<Tuple2<GradoopId, T>> iterator = elements.iterator();
    Tuple2<GradoopId, T> graphIdElement = iterator.next();

    Map<String, PropertyValue> aggregate = AggregateUtil.increment(new HashMap<>(),
      graphIdElement.f1, aggregateFunctions);

    while (iterator.hasNext()) {
      T element = iterator.next().f1;
      aggregate = AggregateUtil.increment(aggregate, element, aggregateFunctions);
    }

    if (!aggregate.isEmpty()) {
      reusePair.f0 = graphIdElement.f0;
      reusePair.f1 = aggregate;
      out.collect(reusePair);
    }
  }
}
