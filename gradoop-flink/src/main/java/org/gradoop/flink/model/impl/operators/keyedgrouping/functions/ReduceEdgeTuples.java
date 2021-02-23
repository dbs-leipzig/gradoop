/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

/**
 * Reduce edge tuples, calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
public class ReduceEdgeTuples<T extends Tuple> extends ReduceElementTuples<T>
  implements GroupCombineFunction<T, T> {

  /**
   * Initialize this reduce function.
   *
   * @param tupleDataOffset    The data offset of the tuple. This will be
   *                           {@value GroupingConstants#EDGE_TUPLE_RESERVED} {@code +} the
   *                           number of the grouping keys.
   * @param aggregateFunctions The vertex aggregate functions.
   */
  public ReduceEdgeTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    super(tupleDataOffset, aggregateFunctions);
  }

  @Override
  public void combine(Iterable<T> values, Collector<T> out) throws Exception {
    reduce(values, out);
  }

  @Override
  public void reduce(Iterable<T> input, Collector<T> out) throws Exception {
    T first = null;
    for (T inputTuple : input) {
      if (first == null) {
        first = inputTuple;
        continue;
      }
      callAggregateFunctions(first, inputTuple);
    }
    out.collect(first);
  }
}
