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
package org.gradoop.flink.algorithms.gelly.connectedcomponents.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Join function to receive structural information of the graph.
 * Builds edge triples with long id's.
 */
@FunctionAnnotation.ForwardedFieldsFirst({"f0->f0", "f1->f1", "f0->f2"})
@FunctionAnnotation.ForwardedFieldsSecond("targetId->f3")
public class CreateLongSourceIds
  implements JoinFunction<Tuple2<Long, GradoopId>, Edge, Tuple4<Long, GradoopId, Long, GradoopId>> {

  /**
   * Reuse object.
   */
  private Tuple4<Long, GradoopId, Long, GradoopId> reuse;

  /**
   * Creates an instance of this join function.
   */
  public CreateLongSourceIds() {
    reuse = new Tuple4<>();
  }

  /**
   * Builds intermediate result.
   *
   * @param tuple holds information about long id and gradoop id of a vertex
   * @param edge holds information about source and target vertices
   * @return tuple <vertexID<Long>>,vertexID<GradoopID>>,sourceID<Long>>,targetID<GradoopID>>
   */
  @Override
  public Tuple4<Long, GradoopId, Long, GradoopId> join(Tuple2<Long, GradoopId> tuple, Edge edge) {
    reuse.f0 = tuple.f0;
    reuse.f1 = tuple.f1;
    reuse.f2 = tuple.f0;
    reuse.f3 = edge.getTargetId();
    return reuse;
  }
}
