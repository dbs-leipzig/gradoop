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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Aggregates the aggregate values of partitions with the same graph id
 */
public class CombinePartitionApplyAggregates implements GroupReduceFunction
  <Tuple2<GradoopId, Map<String, PropertyValue>>, Tuple2<GradoopId, Map<String, PropertyValue>>> {

  /**
   * Aggregate Functions
   */
  private final Set<AggregateFunction> aggregateFunctions;

  /**
   * Constructor.
   *
   * @param aggregateFunctions aggregate functions
   */
  public CombinePartitionApplyAggregates(Set<AggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, Map<String, PropertyValue>>> values,
    Collector<Tuple2<GradoopId, Map<String, PropertyValue>>> out) throws Exception {

    Iterator<Tuple2<GradoopId, Map<String, PropertyValue>>> iterator = values.iterator();

    Tuple2<GradoopId, Map<String, PropertyValue>> aggregate = iterator.next();

    while (iterator.hasNext()) {
      Tuple2<GradoopId, Map<String, PropertyValue>> next = iterator.next();

      for (AggregateFunction aggFunc : aggregateFunctions) {
        String propertyKey = aggFunc.getAggregatePropertyKey();
        PropertyValue nextAgg = next.f1.get(propertyKey);
        if (nextAgg != null) {
          aggregate.f1.compute(propertyKey, (key, agg) -> agg == null ?
            nextAgg : aggFunc.aggregate(agg, nextAgg));
        }
      }
    }

    out.collect(aggregate);
  }
}
