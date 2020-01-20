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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

/**
 * Takes an EPGM edge and creates a tuple, containing the edges source id as unique long id, its
 * target id as GradoopId and its own id as GradoopId.
 */
@FunctionAnnotation.ForwardedFieldsFirst("targetId->f1;id->f2")
@FunctionAnnotation.ForwardedFieldsSecond("f0")
public class LongIdWithEdgeToTupleJoin implements
  JoinFunction<EPGMEdge, Tuple2<Long, GradoopId>, Tuple3<Long, GradoopId, GradoopId>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<Long, GradoopId, GradoopId> reuseTuple;

  /**
   * Creates an instance of LongIdWithEdgeToTupleJoin.
   */
  public LongIdWithEdgeToTupleJoin() {
    this.reuseTuple = new Tuple3<>();
  }

  @Override
  public Tuple3<Long, GradoopId, GradoopId> join(
    EPGMEdge epgmEdge,
    Tuple2<Long, GradoopId> uniqueLongToVertexId) throws Exception {
    reuseTuple.f0 = uniqueLongToVertexId.f0;
    reuseTuple.f1 = epgmEdge.getTargetId();
    reuseTuple.f2 = epgmEdge.getId();
    return reuseTuple;
  }
}
