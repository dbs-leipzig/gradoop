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
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Join edge tuples with the graph sets of their targets
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinEdgeTupleWithTargetGraphs<E extends Edge>
  implements JoinFunction
  <Tuple2<E, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>,
    Tuple3<E, GradoopIdSet, GradoopIdSet>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<E, GradoopIdSet, GradoopIdSet> reuseTuple =
    new Tuple3<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<E, GradoopIdSet, GradoopIdSet> join(
    Tuple2<E, GradoopIdSet> left,
    Tuple2<GradoopId, GradoopIdSet> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = right.f1;
    return reuseTuple;
  }
}
