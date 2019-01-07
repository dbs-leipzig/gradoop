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
package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Map function to create gelly edge from long tuple.
 */
@FunctionAnnotation.ForwardedFields({"f2->f0", "f3->f1"})
public class LongTupleToGellyEdgeWithLongValue
  implements ElementToGellyEdge<Tuple4<Long, GradoopId, Long, Long>, Long, NullValue> {

  /**
   * Reuse object.
   */
  private Edge<Long, NullValue> reuse;

  /**
   * Creates an instance of this map function to create gelly edges.
   */
  public LongTupleToGellyEdgeWithLongValue() {
    reuse = new Edge<>();
    reuse.setValue(NullValue.getInstance());
  }

  /**
   * Create gelly edge.
   *
   * @param tuple identifier of source and target vertex id's.
   * @return gelly edge.
   */
  @Override
  public Edge<Long, NullValue> map(Tuple4<Long, GradoopId, Long, Long> tuple) {
    reuse.setSource(tuple.f2);
    reuse.setTarget(tuple.f3);
    return reuse;
  }
}
