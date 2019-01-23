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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Map function to create gelly vertices from long tuple.
 */
@FunctionAnnotation.ForwardedFields({"f0->f0", "f0->f1"})
public class LongTupleToGellyVertexWithLongValue
  implements ElementToGellyVertex<Tuple2<Long, GradoopId>, Long, Long> {

  /**
   * Reuse object.
   */
  private Vertex<Long, Long> vertex;

  /**
   * Constructor
   */
  public LongTupleToGellyVertexWithLongValue() {
    this.vertex = new Vertex<>();
  }

  /**
   * Map function to create gelly vertices with long key and long value.
   *
   * @param tuple given unique vertex id
   * @return gelly vertex
   */
  @Override
  public Vertex<Long, Long> map(Tuple2<Long, GradoopId> tuple) {
    vertex.setId(tuple.f0);
    vertex.setValue(tuple.f0);
    return vertex;
  }
}
