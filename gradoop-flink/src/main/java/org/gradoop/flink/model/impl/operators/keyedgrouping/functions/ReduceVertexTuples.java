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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

import static org.gradoop.flink.model.impl.operators.keyedgrouping.functions.GroupingConstants.VERTEX_TUPLE_ID;
import static org.gradoop.flink.model.impl.operators.keyedgrouping.functions.GroupingConstants.VERTEX_TUPLE_SUPERID;

/**
 * Reduce vertex tuples, assigning a super vertex ID and calculating aggregate values. This function outputs
 * two kinds of tuples:
 * <ul>
 *   <li>A tuple storing the vertex ID, the ID a super vertex (to be created afterwards), keys and
 *   aggregate values (which are cleared after aggregation, since they are not used after aggregation).</li>
 *   <li>A tuple representing the super vertex, storing the keys and aggregation results for the group.
 *   The super vertex is identified by having the same vertex ID and super vertex ID.</li>
 * </ul>
 *
 * @param <T> The tuple type.
 */
public class ReduceVertexTuples<T extends Tuple> extends ReduceElementTuples<T> {

  /**
   * Initialize this reduce function.
   *
   * @param tupleDataOffset    The data offset of the tuple. This will be
   *                           {@value GroupingConstants#VERTEX_TUPLE_RESERVED} {@code +}
   *                           the number of the grouping keys.
   *
   * @param aggregateFunctions The vertex aggregate functions.
   */
  public ReduceVertexTuples(int tupleDataOffset, List<AggregateFunction> aggregateFunctions) {
    super(tupleDataOffset, aggregateFunctions);
  }

  @Override
  public void reduce(Iterable<T> input, Collector<T> out) throws Exception {
    T superVertexTuple = null;
    GradoopId superVertexId = GradoopId.get();

    for (T inputTuple : input) {
      if (superVertexTuple == null) {
        // Copy the first tuple to be used as the super-vertex tuple.
        superVertexTuple = inputTuple.copy();
      } else {
        // Call aggregate functions for every other tuple of the group.
        callAggregateFunctions(superVertexTuple, inputTuple);
      }
      // Assign the super-vertex ID.
      inputTuple.setField(superVertexId, VERTEX_TUPLE_SUPERID);
      // Return the updated tuple, used to extract the mapping later.
      out.collect(inputTuple);
    }
    // Return the super vertex.
    if (superVertexTuple == null) {
      // This should not happen, since the reduce function can not be called on an empty group.
      throw new IllegalStateException(
        "Super-vertex was not initialized. Do not call this function on empty groups.");
    }
    superVertexTuple.setField(superVertexId, VERTEX_TUPLE_ID);
    superVertexTuple.setField(superVertexId, VERTEX_TUPLE_SUPERID);
    out.collect(superVertexTuple);
  }
}
