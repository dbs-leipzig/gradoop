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
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Calculates sum aggregates of multiple AggregateFunctions.
 */
public class AggregateFunctions
  extends RichGroupReduceFunction<WithCount<int[]>, WithCount<int[]>>
  implements GroupCombineFunction<WithCount<int[]>, WithCount<int[]>> {

  /**
   * Used fields
   */
  private int[] field;
  /**
   * Used AggregationFunctions
   */
  private final AggregationFunction<Long>[] aggregationFunctions;

  /**
   * Calculates sum aggregate of multiple AggregationFunction.
   *
   * @param aggregationFunctions hui hui
   * @param field hui hui
   */
  public AggregateFunctions(AggregationFunction<Long>[] aggregationFunctions, int[] field) {
    this.field = field;
    this.aggregationFunctions = aggregationFunctions;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    for (AggregationFunction<Long> aggFunction : aggregationFunctions) {
      aggFunction.initializeAggregate();
    }
  }

  @Override
  public void combine(Iterable<WithCount<int[]>> iterable,
    Collector<WithCount<int[]>> collector) throws Exception {
    reduce(iterable, collector);
  }

  @Override
  public void reduce(Iterable<WithCount<int[]>> iterable,
    Collector<WithCount<int[]>> collector) throws Exception {
    WithCount<int[]> result = null;
    for (WithCount<int[]> tuple : iterable) {
      result = tuple;

      for (int i = 0; i < field.length; i++) {
        Long value = tuple.getField(field[i]);
        aggregationFunctions[i].aggregate(value);
      }
    }

    for (int i = 0; i < field.length; i++) {
      Long aggVal = aggregationFunctions[i].getAggregate();
      assert result != null;
      result.setField(aggVal, field[i]);
      aggregationFunctions[i].initializeAggregate();
    }

    collector.collect(result);
  }
}
