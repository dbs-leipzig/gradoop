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
package org.gradoop.flink.algorithms.gelly.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps Gradoop Vertex to a Gelly vertex with the {@link GradoopId} as its id.
 *
 * @param <V> Gradoop Vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class VertexToGellyVertexWithNullValue<V extends Vertex> implements VertexToGellyVertex<V, NullValue> {
  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, NullValue> reuseVertex;

  /**
   * Constructor.
   */
  public VertexToGellyVertexWithNullValue() {
    reuseVertex = new org.apache.flink.graph.Vertex<>();
    reuseVertex.setValue(NullValue.getInstance());
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, NullValue> map(V vertex) {
    reuseVertex.setId(vertex.getId());
    return reuseVertex;
  }
}
