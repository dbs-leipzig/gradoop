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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A join function updating the target id of an edge from a mapping in the form of a pair with fields:
 * <ol start="0">
 *   <li>the old target ID</li>
 *   <li>the new target ID</li>
 * </ol>
 * Note that this function can be used with an outer join, in that case the ID will not be updated.
 *
 * @param <E> The edge type.
 */
@FunctionAnnotation.ForwardedFieldsFirst("id;label;properties;sourceId;graphIds")
@FunctionAnnotation.ReadFieldsSecond("*")
public class UpdateEdgeTarget<E extends Edge> implements JoinFunction<E, Tuple2<GradoopId, GradoopId>, E> {

  @Override
  public E join(E edge, Tuple2<GradoopId, GradoopId> mapping) {
    if (mapping != null) {
      edge.setTargetId(mapping.f1);
    }
    return edge;
  }
}
