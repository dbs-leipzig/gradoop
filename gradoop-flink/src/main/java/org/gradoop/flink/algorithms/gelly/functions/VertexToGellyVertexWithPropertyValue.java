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
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Maps Gradoop vertex to a Gelly vertex consisting of the {@link GradoopId} and a {@link PropertyValue}.
 *
 * @param <V> Gradoop Vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class VertexToGellyVertexWithPropertyValue<V extends Vertex>
  implements VertexToGellyVertex<V, PropertyValue> {
  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, PropertyValue> reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexWithPropertyValue(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new org.apache.flink.graph.Vertex<>();
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, PropertyValue> map(V vertex) throws Exception {
    reuseVertex.setId(vertex.getId());
    reuseVertex.setValue(vertex.getPropertyValue(propertyKey));
    return reuseVertex;
  }
}
