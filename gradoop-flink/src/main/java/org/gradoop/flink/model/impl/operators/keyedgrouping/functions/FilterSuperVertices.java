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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A filter function used to select super vertices from the set of vertex-tuples.
 * Super-vertices are identified by them having the same ID and super ID.
 *
 * @param <T> The type of the vertex-tuples.
 */
@FunctionAnnotation.ReadFields({"f0", "f1"})
public class FilterSuperVertices<T extends Tuple> implements FilterFunction<T> {

  @Override
  public boolean filter(T t) throws Exception {
    final GradoopId id = t.getField(GroupingConstants.VERTEX_TUPLE_ID);
    final GradoopId superId = t.getField(GroupingConstants.VERTEX_TUPLE_SUPERID);
    return id.equals(superId);
  }
}
