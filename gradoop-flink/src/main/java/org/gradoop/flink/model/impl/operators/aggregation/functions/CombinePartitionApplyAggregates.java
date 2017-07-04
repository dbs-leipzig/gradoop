/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

/**
 * (graphId,partitionAggregateValue),.. => (graphId,globalAggregateValue),..
 */
public class CombinePartitionApplyAggregates implements GroupReduceFunction
  <Tuple2<GradoopId, PropertyValue>, Tuple2<GradoopId, PropertyValue>> {

  /**
   * Aggregate Function
   */
  private final AggregateFunction aggFunc;

  /**
   * Constructor.
   *
   * @param aggregateFunction aggregate function
   */
  public CombinePartitionApplyAggregates(AggregateFunction aggregateFunction) {

    this.aggFunc = aggregateFunction;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, PropertyValue>> values,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {

    Iterator<Tuple2<GradoopId, PropertyValue>> iterator = values.iterator();

    Tuple2<GradoopId, PropertyValue> aggregate = iterator.next();

    while (iterator.hasNext()) {
      aggregate.f1 = aggFunc.aggregate(aggregate.f1, iterator.next().f1);
    }

    out.collect(aggregate);
  }
}
