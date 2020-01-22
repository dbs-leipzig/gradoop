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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

/**
 * Reduce vertex tuples, calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
abstract class ReduceElementTuples<T extends Tuple> implements GroupReduceFunction<T, T> {

  /**
   * The data offset for tuples. Aggregate values are expected to start at this index.
   */
  private final int tupleDataOffset;

  /**
   * The aggregate functions.
   */
  private final List<AggregateFunction> aggregateFunctions;

  /**
   * Instantiate this base class, setting the data offset and aggregate functions.
   *
   * @param tupleDataOffset    The data offset for aggregate values in the tuple.
   * @param aggregateFunctions The aggregate functions.
   */
  ReduceElementTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    this.aggregateFunctions = aggregateFunctions;
  }

  /**
   * Calculate aggregate functions and update tuple fields.
   *
   * @param superTuple       The tuple storing the current aggregate values.
   * @param inputTuple       The tuple storing the increment values.
   */
  void callAggregateFunctions(T superTuple, T inputTuple) {
    // Calculate aggregate values.
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      final PropertyValue aggregate = superTuple.getField(i + tupleDataOffset);
      final PropertyValue increment = inputTuple.getField(i + tupleDataOffset);
      // Delete the increment from the input tuple as it is not needed anymore.
      inputTuple.setField(PropertyValue.NULL_VALUE, i + tupleDataOffset);
      // Do not aggregate if the increment is null.
      if (!increment.equals(PropertyValue.NULL_VALUE)) {
        if (aggregate.equals(PropertyValue.NULL_VALUE)) {
          // If the aggregate is null, use the increment as the new initial value.
          superTuple.setField(increment, i + tupleDataOffset);
        } else {
          superTuple.setField(aggregateFunctions.get(i).aggregate(aggregate, increment), i + tupleDataOffset);
        }
      }
    }
  }
}
