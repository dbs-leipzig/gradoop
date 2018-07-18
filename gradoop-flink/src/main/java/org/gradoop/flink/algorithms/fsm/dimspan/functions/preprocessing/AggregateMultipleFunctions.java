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
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Arrays;

/**
 * Calculates sum aggregates of multiple AggregateFunctions.
 */
//
// NOTE: The code in this file is based on code from the
// Apache Flink project, licensed under the Apache License v 2.0
//
// (https://github.com/apache/flink/blob/master/flink-java/src/main/java/org/apache/flink/api
// /java/operators/AggregateOperator.java#L244)
public class AggregateMultipleFunctions
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
   * @param aggregationFunctions array of AggregateFunctions
   * @param field value fields
   */
  public AggregateMultipleFunctions(AggregationFunction<Long>[] aggregationFunctions, int[] field) {
    Preconditions.checkArgument(aggregationFunctions.length == field.length);
    Preconditions.checkArgument(aggregationFunctions.length > 0);
    this.field = Arrays.copyOf(field, field.length);
    this.aggregationFunctions = Arrays.copyOf(aggregationFunctions, aggregationFunctions.length);
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

      // calculates new aggregates
      for (int i = 0; i < field.length; i++) {
        Long value = tuple.getField(field[i]);
        aggregationFunctions[i].aggregate(value);
      }
    }

    // forward new WithCount with aggregated value
    for (int i = 0; i < field.length; i++) {
      Long aggregatedValue = aggregationFunctions[i].getAggregate();
      result.setField(aggregatedValue, field[i]);
      aggregationFunctions[i].initializeAggregate();
    }

    collector.collect(result);
  }
}
