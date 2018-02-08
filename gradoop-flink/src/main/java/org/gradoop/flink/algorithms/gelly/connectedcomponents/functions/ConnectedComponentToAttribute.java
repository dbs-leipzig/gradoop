/**
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
package org.gradoop.flink.algorithms.gelly.connectedcomponents.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Stores the component id (as a {@link GradoopId} of one of the components vertices) as a property
 * in the vertex.
 */
public class ConnectedComponentToAttribute
  implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, GradoopId>, Vertex, Vertex> {

  /**
   * Property to store the component id in.
   */
  private final String componentProperty;

  /**
   * Stores the connected components result as a Property.
   *
   * @param componentProperty Property name.
   */
  public ConnectedComponentToAttribute(String componentProperty) {
    this.componentProperty = componentProperty;
  }

  @Override
  public Vertex join(org.apache.flink.graph.Vertex<GradoopId, GradoopId> gellyVertex,
    Vertex gradoopVertex) {
    gradoopVertex.setProperty(componentProperty, gellyVertex.getValue());
    return gradoopVertex;
  }
}
