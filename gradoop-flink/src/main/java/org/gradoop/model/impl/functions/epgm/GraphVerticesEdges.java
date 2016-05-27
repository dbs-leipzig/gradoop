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

package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

/**
 * (graphId, {vertex,..}) |><| (graphID, {edge,..})
 * => (graphId, {vertex,..}, {edge,..})
 *
 * @param <V> vertex type
 * @param <E> edge type
 */
public class GraphVerticesEdges<V extends EPGMVertex, E extends EPGMEdge>
  implements JoinFunction<Tuple2<GradoopId, Set<V>>, Tuple2<GradoopId, Set<E>>,
  Tuple3<GradoopId, Set<V>, Set<E>>> {

  @Override
  public Tuple3<GradoopId, Set<V>, Set<E>> join(
    Tuple2<GradoopId, Set<V>> vertices, Tuple2<GradoopId, Set<E>> edges) throws
    Exception {
    return new Tuple3<>(vertices.f0, vertices.f1, edges.f1);
  }
}
