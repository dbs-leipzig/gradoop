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

/**
 * Join function to receive structural information of the graph.
 * Builds edge triples with long id's.
 */
@FunctionAnnotation.ForwardedFieldsFirst({"f0->f0", "f1->f1", "f2->f2"})
@FunctionAnnotation.ForwardedFieldsSecond("f0->f3")
public class CreateLongTargetIds
  implements JoinFunction<Tuple4<Long, GradoopId, Long, GradoopId>, Tuple2<Long, GradoopId>,
  Tuple4<Long, GradoopId, Long, Long>> {

  /**
   * Reuse object.
   */
  private Tuple4<Long, GradoopId, Long, Long> reuse;

  /**
   * Creates an instance of this join function.
   */
  public CreateLongTargetIds() {
    reuse = new Tuple4<>();
  }

  /**
   * Builds triples with long id's for every edge.
   *
   * @param edgeTuple intermediate join result.
   * @param uniqueVertex unique vertex identifier.
   * @return tuple <vertexID<Long>>,vertexID<GradoopID>>,sourceID<Long>>,targetID<Long>>
   */
  @Override
  public Tuple4<Long, GradoopId, Long, Long> join(
    Tuple4<Long, GradoopId, Long, GradoopId> edgeTuple, Tuple2<Long, GradoopId> uniqueVertex) {
    reuse.f0 = edgeTuple.f0;
    reuse.f1 = edgeTuple.f1;
    reuse.f2 = edgeTuple.f2;
    reuse.f3 = uniqueVertex.f0;
    return reuse;
  }
}
