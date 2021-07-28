/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.TreeMap;

/**
 * Join function that extends the calculated degree tree for each vertex with the time interval of the vertex.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0; f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2;f2->f3")
public class ExtendVertexDataWithInterval
  implements JoinFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>, Tuple3<GradoopId, Long, Long>,
  Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> reuse;

  /**
   * Creates an instance of this join function.
   */
  public ExtendVertexDataWithInterval() {
    this.reuse = new Tuple4<>();
  }

  @Override
  public Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> join(
    Tuple2<GradoopId, TreeMap<Long, Integer>> left,
    Tuple3<GradoopId, Long, Long> right) throws Exception {

    reuse.f0 = left.f0;
    reuse.f1 = left.f1;
    reuse.f2 = right.f1;
    reuse.f3 = right.f2;

    return reuse;
  }
}
