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
package org.gradoop.flink.algorithms.gelly.randomjump.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Replaces the target GradoopId with the unique long id this target vertex is mapped to.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f2")
@FunctionAnnotation.ForwardedFieldsSecond("f0->f1")
public class ReplaceTargetWithLongIdJoin implements
  JoinFunction<Tuple3<Long, GradoopId, GradoopId>, Tuple2<Long, GradoopId>,
    Tuple3<Long, Long, GradoopId>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<Long, Long, GradoopId> reuseTuple;

  /**
   * Creates an instance of ReplaceTargetWithLongIdJoin.
   */
  public ReplaceTargetWithLongIdJoin() {
    this.reuseTuple = new Tuple3<>();
  }

  @Override
  public Tuple3<Long, Long, GradoopId> join(Tuple3<Long, GradoopId, GradoopId> edgeTuple,
    Tuple2<Long, GradoopId> uniqueLongToVertexId) throws Exception {
    reuseTuple.f0 = edgeTuple.f0;
    reuseTuple.f1 = uniqueLongToVertexId.f0;
    reuseTuple.f2 = edgeTuple.f2;
    return reuseTuple;
  }
}
