/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Sets all graph ids for master data vertices. The graph ids of each edge of which the vertex is
 * the target of are added to the vertex.
 */
public class GraphIdsFromEdges implements CoGroupFunction<Vertex, Edge, Vertex> {

  @Override
  public void coGroup(Iterable<Vertex> vertices, Iterable<Edge> edges,
    Collector<Vertex> collector) throws Exception {
    for (Vertex vertex : vertices) {
      for (Edge edge : edges) {
        for (GradoopId gradoopId : edge.getGraphIds()) {
          vertex.addGraphId(gradoopId);
        }
      }
      collector.collect(vertex);
    }
  }
}
