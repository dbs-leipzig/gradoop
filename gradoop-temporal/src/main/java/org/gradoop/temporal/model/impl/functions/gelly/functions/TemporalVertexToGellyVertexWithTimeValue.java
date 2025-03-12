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
package org.gradoop.temporal.model.impl.functions.gelly.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 * Maps Temporal vertex to a Gelly vertex consisting of the {@link GradoopId} and Valid Time.
 *
 * @param <V> Gradoop Vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class TemporalVertexToGellyVertexWithTimeValue<V extends TemporalVertex>
  implements TemporalVertexToGellyVertex<V, Tuple2<Long, Long>> {
  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, Tuple2<Long, Long>> reuseVertex;

  /**
   * Constructor
   */
  public TemporalVertexToGellyVertexWithTimeValue() {
    this.reuseVertex = new org.apache.flink.graph.Vertex<>();
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, Tuple2<Long, Long>> map(V vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    reuseVertex.setValue(vertex.getValidTime());
    return reuseVertex;
  }
}
