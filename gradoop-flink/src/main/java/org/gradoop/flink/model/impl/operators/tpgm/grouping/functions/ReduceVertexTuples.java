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
package org.gradoop.flink.model.impl.operators.tpgm.grouping.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

/**
 * Reduce vertex tuples, assigning a super vertex ID and calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
public class ReduceVertexTuples<T extends Tuple>
  implements GroupReduceFunction<T, T>, TemporalGroupingConstants {

  /**
   * The data offset for tuples. Aggregate values are expected to start at this index.
   */
  private final int tupleDataOffset;

  /**
   * The vertex aggregate functions.
   */
  private final List<AggregateFunction> aggregateFunctions;

  /**
   * Reduce object instantiations.
   */
  private T reuseSuperVertex;

  /**
   * Initialize this reduce function.
   *
   * @param tupleDataOffset    The data offset of the tuple. This will be
   *                           {@value VERTEX_TUPLE_RESERVED} {@code +} the number of the grouping
   *                           keys.
   * @param aggregateFunctions The vertex aggregate functions.
   */
  public ReduceVertexTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    this.tupleDataOffset = tupleDataOffset;
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void reduce(Iterable<T> input, Collector<T> out) throws Exception {
    boolean isFirst = true;
    GradoopId superVertexId = GradoopId.get();

    for (T inputTuple : input) {
      if (isFirst) {
        reuseSuperVertex = inputTuple.copy();
        isFirst = false;
      } else {
        // Calculate aggregate values.
        for (int i = 0; i < aggregateFunctions.size(); i++) {
          final PropertyValue aggregate = reuseSuperVertex.getField(i + tupleDataOffset);
          final PropertyValue increment = inputTuple.getField(i + tupleDataOffset);
          reuseSuperVertex.setField(aggregateFunctions.get(i).aggregate(aggregate, increment),
            i + tupleDataOffset);
        }
      }
      inputTuple.setField(superVertexId, VERTEX_TUPLE_SUPERID);
      // Return the updated tuple, used to extract the mapping later.
      out.collect(inputTuple);
    }
    // Return a super vertex.
    reuseSuperVertex.setField(superVertexId, VERTEX_TUPLE_ID);
    reuseSuperVertex.setField(superVertexId, VERTEX_TUPLE_SUPERID);
    out.collect(reuseSuperVertex);
  }
}
