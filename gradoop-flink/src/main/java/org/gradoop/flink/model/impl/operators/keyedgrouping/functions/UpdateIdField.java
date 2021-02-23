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
 * Update a tuple field with a certain index to a value from another tuple.
 * The left side of this join function is the tuple to be updated and the right side a {@link Tuple2}
 * with the old and new value.
 *
 * @param <T> The input- and result-tuple type.
 */
public class UpdateIdField<T extends Tuple> implements JoinFunction<T, Tuple2<GradoopId, GradoopId>, T> {

  /**
   * The index of the field to update.
   */
  private final int index;

  /**
   * Create a new instance of this update function.
   *
   * @param index The index of the field to update.
   */
  public UpdateIdField(int index) {
    this.index = index;
  }

  @Override
  public T join(T inputTuple, Tuple2<GradoopId, GradoopId> updateValue) throws Exception {
    inputTuple.setField(updateValue.f1, index);
    return inputTuple;
  }
}
