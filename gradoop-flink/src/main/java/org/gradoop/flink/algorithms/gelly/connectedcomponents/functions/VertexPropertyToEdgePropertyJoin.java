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
package org.gradoop.flink.algorithms.gelly.connectedcomponents.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Reads a vertex property and stores it to an edge property, using the given property key.
 * If the edge already has this property, compare the edge value to the vertex value and store the
 * smaller property value.
 */
public class VertexPropertyToEdgePropertyJoin implements JoinFunction<Edge, Vertex, Edge> {

  /**
   * Property key to store the property.
   */
  private final String propertyKey;

  /**
   * Constructor with property key.
   *
   * @param propertyKey Property key to store the property.
   */
  public VertexPropertyToEdgePropertyJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Edge join(Edge edge, Vertex vertex) throws Exception {
    PropertyValue newEdgeValue = vertex.getPropertyValue(propertyKey);
    if (edge.hasProperty(propertyKey)) {
      if (edge.getPropertyValue(propertyKey).compareTo(newEdgeValue) < 0) {
        return edge;
      }
    }
    edge.setProperty(propertyKey, newEdgeValue);
    return edge;
  }
}
