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
package org.gradoop.flink.model.impl.operators.groupingng.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

/**
 * Reduce edge tuples, calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
public class ReduceEdgeTuples<T extends Tuple> implements GroupReduceFunction<T, T> {

  /**
   * The data offset for tuples. Aggregate values are expected to start at this index.
   */
  private final int tupleDataOffset;

  /**
   * The edge aggregate functions.
   */
  private final List<AggregateFunction> aggregateFunctions;

  /**
   * Initialize this reduce function.
   *
   * @param tupleDataOffset    The data offset of the tuple. This will be
   *                           {@value TemporalGroupingConstants#EDGE_TUPLE_RESERVED} {@code +} the
   *                           number of the grouping keys.
   * @param aggregateFunctions The vertex aggregate functions.
   */
  public ReduceEdgeTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void reduce(Iterable<T> input, Collector<T> out) throws Exception {
    T first = null;
    for (T inputTuple : input) {
      if (first == null) {
        first = inputTuple;
        continue;
      }
      for (int i = 0; i < aggregateFunctions.size(); i++) {
        final PropertyValue aggregate = first.getField(i + tupleDataOffset);
        final PropertyValue increment = inputTuple.getField(i + tupleDataOffset);
        first.setField(aggregateFunctions.get(i).aggregate(aggregate, increment),
          i + tupleDataOffset);
      }
    }
    out.collect(first);
  }
}
