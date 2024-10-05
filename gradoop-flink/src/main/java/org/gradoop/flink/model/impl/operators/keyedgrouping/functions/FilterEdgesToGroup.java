/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A filter function accepting all edges that were not marked for retention.
 *
 * @param <T> The edge tuple type.
 */
public class FilterEdgesToGroup<T extends Tuple> implements FilterFunction<T> {

  @Override
  public boolean filter(T tuple) {
    return tuple.getField(GroupingConstants.EDGE_TUPLE_ID).equals(GradoopId.NULL_VALUE);
  }
}
