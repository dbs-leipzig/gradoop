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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Updates the an ID field of an edge tuple to the ID of the corresponding super vertex.<p>
 * This function is used when retention of ungrouped vertices is enabled. In this case edge tuples have an
 * additional ID field. This field will initially be equal to the ID of the edge. When the ID field is
 * updated by this function, that field will be set to {@link GradoopId#NULL_VALUE} instead.
 *
 * @param <T> The edge tuple type.
 */
public class UpdateIdFieldAndMarkTuple<T extends Tuple>
  implements JoinFunction<T, Tuple2<GradoopId, GradoopId>, T> {

  /**
   * The index of the field to update.
   */
  private final int index;

  /**
   * Create a new instance of this update function.
   *
   * @param index The index of the field to update (without offset).
   */
  public UpdateIdFieldAndMarkTuple(int index) {
    if (index < 0) {
      throw new IllegalArgumentException("Index can not be negative.");
    }
    this.index = index + GroupingConstants.EDGE_RETENTION_OFFSET;
  }

  @Override
  public T join(T edgeTuple, Tuple2<GradoopId, GradoopId> mapping) {
    if (!mapping.f0.equals(mapping.f1)) {
      // Mark the tuple and update the field, if the mapping would actually change it.
      GradoopId.NULL_VALUE.copyTo(edgeTuple.getField(GroupingConstants.EDGE_TUPLE_ID));
      edgeTuple.setField(mapping.f1, index);
    }
    return edgeTuple;
  }
}
