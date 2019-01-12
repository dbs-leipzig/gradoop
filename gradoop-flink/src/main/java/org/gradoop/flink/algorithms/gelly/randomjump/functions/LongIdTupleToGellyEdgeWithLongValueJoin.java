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
import org.apache.flink.graph.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a Gelly edge with its long index as its value.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
@FunctionAnnotation.ForwardedFieldsSecond("f0->f2")
public class LongIdTupleToGellyEdgeWithLongValueJoin implements
  JoinFunction<Tuple3<Long, Long, GradoopId>, Tuple2<Long, GradoopId>, Edge<Long, Long>> {

  /**
   * Reduce object instantiation.
   */
  private final Edge<Long, Long> reuseEdge;

  /**
   * Creates an instance of LongIdTupleToGellyEdgeWithLongValueJoin.
   */
  public LongIdTupleToGellyEdgeWithLongValueJoin() {
    this.reuseEdge = new Edge<>();
  }

  @Override
  public Edge<Long, Long> join(Tuple3<Long, Long, GradoopId> edgeTuple,
    Tuple2<Long, GradoopId> indexToEdgeId) throws Exception {
    reuseEdge.setSource(edgeTuple.f0);
    reuseEdge.setTarget(edgeTuple.f1);
    reuseEdge.setValue(indexToEdgeId.f0);
    return reuseEdge;
  }
}
