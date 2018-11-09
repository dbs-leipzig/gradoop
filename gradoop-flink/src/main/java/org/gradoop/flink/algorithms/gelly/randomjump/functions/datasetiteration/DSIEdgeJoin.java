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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.List;

/**
 * Joins an EPGM edge with a DataSetIteration tuple if the tuples source vertex id is the edges
 * source id. Assigns a boolean property to the edge, determining if this edge was visited. The
 * property is set to {@code true} if the {@code Set<Tuple2<Long, GradoopId>>} in the tuple
 * contains the target id of the edge, set to {@code false} otherwise.
 */
public class DSIEdgeJoin implements JoinFunction<Edge, IterativeTuple, Edge> {

  /**
   * Key for the boolean property of the edge
   */
  private final String propertyKey;

  /**
   * Creates an instance of DSIEdgeJoin with a given key for the boolean
   * property value.
   *
   * @param propertyKey propertyKey Key for the boolean property value
   */
  public DSIEdgeJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Edge join(Edge epgmEdge, IterativeTuple iterativeTuple) throws Exception {
    List<IterativeTuple.EdgeType> edges = iterativeTuple.getVisitedEdges();
    boolean visited = false;
    for (IterativeTuple.EdgeType edge : edges) {
      if (epgmEdge.getTargetId().equals(edge.getTargetId())) {
        visited = true;
      }
    }
    epgmEdge.setProperty(propertyKey, visited);
    return epgmEdge;
  }
}
