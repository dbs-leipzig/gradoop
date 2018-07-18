/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Join an edge tuple with a tuple containing the target vertex id of this edge
 * and its new graphs.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinWithTargetGraphIdSet
  implements JoinFunction<
  Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet>,
  Tuple2<GradoopId, GradoopIdSet>,
  Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet> reuseTuple
    = new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet> join(
    Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet> edge,
    Tuple2<GradoopId, GradoopIdSet> vertex) throws
    Exception {
    reuseTuple.f0 = edge.f0;
    reuseTuple.f1 = edge.f1;
    reuseTuple.f2 = vertex.f1;
    reuseTuple.f3 = edge.f3;
    return reuseTuple;
  }
}
