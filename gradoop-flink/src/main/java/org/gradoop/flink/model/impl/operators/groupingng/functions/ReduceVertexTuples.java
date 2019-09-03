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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.List;

import static org.gradoop.flink.model.impl.operators.groupingng.functions.GroupingNGConstants.VERTEX_TUPLE_ID;
import static org.gradoop.flink.model.impl.operators.groupingng.functions.GroupingNGConstants.VERTEX_TUPLE_SUPERID;

/**
 * Reduce vertex tuples, assigning a super vertex ID and calculating aggregate values.
 *
 * @param <T> The tuple type.
 */
public class ReduceVertexTuples<T extends Tuple> extends ReduceElementTuples<T> {

  /**
   * Initialize this reduce function.
   *
   * @param tupleDataOffset    The data offset of the tuple. This will be
   *                           {@value GroupingNGConstants#VERTEX_TUPLE_RESERVED} {@code +}
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
        superVertexTuple = inputTuple.copy();
      } else {
        callAggregateFunctions(superVertexTuple, inputTuple);
      }
      inputTuple.setField(superVertexId, VERTEX_TUPLE_SUPERID);
      // Return the updated tuple, used to extract the mapping later.
      out.collect(inputTuple);
    }
    // Return a super vertex.
    superVertexTuple.setField(superVertexId, VERTEX_TUPLE_ID);
    superVertexTuple.setField(superVertexId, VERTEX_TUPLE_SUPERID);
    out.collect(superVertexTuple);
  }
}
