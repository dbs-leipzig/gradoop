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
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Join a tuple of an element id and a graph id with a dictionary mapping each
 * graph to a new graph. Result is a tuple of the id of the element and the id
 * of the new graph.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinTuplesWithNewGraphs
  implements JoinFunction<
  Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>> {


  /**
   * Reduce object instantiations
   */
  private Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = right.f1;
    return reuseTuple;
  }
}
