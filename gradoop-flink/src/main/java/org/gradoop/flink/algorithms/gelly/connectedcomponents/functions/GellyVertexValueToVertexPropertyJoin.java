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
package org.gradoop.flink.algorithms.gelly.connectedcomponents.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Stores the gelly vertex value (a {@link GradoopId}) as property with the given property key in
 * the gradoop vertex.
 *
 * @param <V> Gradoop Vertex type
 */
public class GellyVertexValueToVertexPropertyJoin<V extends Vertex>
  implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, GradoopId>, V, V> {

  /**
   * Property key to store the gelly vertex value.
   */
  private final String propertyKey;

  /**
   * Stores the gelly vertex value as a property.
   *
   * @param propertyKey Property key to store the gelly vertex value
   */
  public GellyVertexValueToVertexPropertyJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public V join(org.apache.flink.graph.Vertex<GradoopId, GradoopId> gellyVertex, V vertex) {
    vertex.setProperty(propertyKey, gellyVertex.getValue());
    return vertex;
  }
}
