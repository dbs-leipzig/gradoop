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

/**
 * Join the edge tuples with the new graph ids.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f2->f2")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f3")
public class EdgesWithNewGraphsTuple
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>,
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> reuseTuple =
    new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> join(
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = left.f2;
    reuseTuple.f3 = right.f1;
    return reuseTuple;
  }
}
