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
package org.gradoop.flink.algorithms.gelly.shortestpaths.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Stores the minimum distance as a property in vertex.
 */
public class SingleSourceShortestPathsAttribute
  implements JoinFunction<org.apache.flink.graph.Vertex<GradoopId, Double>, Vertex, Vertex> {

  /**
   * Property to store the minimum distance in.
   */
  private final String shortestPathProperty;

  /**
   * Stores the shortest distance to the source vertex as a property.
   *
   * @param shortestPathProperty property key to store shortest path in
   */
  public SingleSourceShortestPathsAttribute(String shortestPathProperty) {
    this.shortestPathProperty = shortestPathProperty;
  }

  @Override
  public Vertex join(org.apache.flink.graph.Vertex<GradoopId, Double> gellyVertex,
    Vertex gradoopVertex) {
    gradoopVertex.setProperty(shortestPathProperty, gellyVertex.getValue());
    return gradoopVertex;
  }
}
