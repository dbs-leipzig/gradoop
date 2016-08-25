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

package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Set;

/**
 * (graphId, {vertex,..}) |><| (graphID, {edge,..})
 * => (graphId, {vertex,..}, {edge,..})
 */
public class GraphVerticesEdges implements JoinFunction<
  Tuple2<GradoopId, Set<Vertex>>, Tuple2<GradoopId, Set<Edge>>,
  Tuple3<GradoopId, Set<Vertex>, Set<Edge>>> {

  @Override
  public Tuple3<GradoopId, Set<Vertex>, Set<Edge>> join(
    Tuple2<GradoopId, Set<Vertex>> vertexPair,
    Tuple2<GradoopId, Set<Edge>> edgePair) throws Exception {

    GradoopId graphId;
    Set<Vertex> vertices;
    Set<Edge> edges;

    if (vertexPair == null) {
      graphId = edgePair.f0;
      vertices = Sets.newHashSetWithExpectedSize(0);
      edges = edgePair.f1;
    } else {
      graphId = vertexPair.f0;
      vertices = vertexPair.f1;
      edges = edgePair == null ? getEmptyEdgeSet() : edgePair.f1;
    }

    return new Tuple3<>(graphId, vertices, edges);
  }

  private Set<Edge> getEmptyEdgeSet() {
    return Sets.newHashSetWithExpectedSize(0);
  }
}
