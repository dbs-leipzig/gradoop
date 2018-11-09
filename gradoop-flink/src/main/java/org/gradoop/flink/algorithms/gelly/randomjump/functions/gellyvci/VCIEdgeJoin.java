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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.List;

/**
 * Joins an EPGM edge with a Gelly vertex if the vertex id is the edges source id. Assigns a
 * boolean property to the edge, determining if this edge was visited. The property is set to
 * {@code true} if the {@code Set<GradoopId>} in the vertex value contains the target id of the
 * edge, set to {@code false} otherwise.
 */
public class VCIEdgeJoin implements JoinFunction<Edge, Vertex<GradoopId, VCIVertexValue>, Edge> {

  /**
   * Key for the boolean property of the edge
   */
  private final String propertyKey;

  /**
   * Creates an instance of VCIEdgeJoin with a given key for the boolean
   * property value.
   *
   * @param propertyKey propertyKey Key for the boolean property value
   */
  public VCIEdgeJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Edge join(Edge edge, Vertex<GradoopId, VCIVertexValue> gellyVertex) throws Exception {
    List<GradoopId> visitedNeighbors = gellyVertex.getValue().f1;
    boolean visited = false;
    for (GradoopId neighborId : visitedNeighbors) {
      if (edge.getTargetId().equals(neighborId)) {
        visited = true;
      }
    }
    edge.setProperty(propertyKey, visited);
    return edge;
  }
}
