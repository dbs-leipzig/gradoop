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
package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Base class for overlap operators that contains common logic.
 *
 * @see Overlap
 * @see ReduceOverlap
 */
public abstract class OverlapBase {

  /**
   * Filters vertices based on the given graph identifiers.
   *
   * @param vertices  vertices
   * @param ids       graph identifiers
   * @return filtered vertices
   */
  protected DataSet<Vertex> getVertices(DataSet<Vertex> vertices,
    DataSet<GradoopId> ids) {
    return vertices
      .filter(new InAllGraphsBroadcast<Vertex>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }

  /**
   * Filters edges based on the given graph identifiers.
   *
   * @param edges edges
   * @param ids   graph identifiers
   * @return filtered edges
   */
  protected DataSet<Edge> getEdges(DataSet<Edge> edges,
    DataSet<GradoopId> ids) {
    return edges
      .filter(new InAllGraphsBroadcast<Edge>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }
}
