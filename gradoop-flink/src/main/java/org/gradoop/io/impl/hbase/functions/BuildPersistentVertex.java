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

package org.gradoop.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;

import java.util.Set;

/**
 * Used to create persistent vertex data from vertex data and
 * outgoing/incoming edge data.
 *
 * @param <V>   EPGM vertex type
 * @param <E>   EPGM edge type
 * @param <PV>  EPGM persistent vertex type
 */
public class BuildPersistentVertex
  <V extends Vertex, E extends Edge, PV extends PersistentVertex<E>>
  implements CoGroupFunction<Tuple2<V, Set<E>>, Tuple2<GradoopId, Set<E>>,
    PersistentVertex<E>> {

  /**
   * Persistent vertex data factory.
   */
  private final PersistentVertexFactory<V, E, PV> vertexFactory;

  /**
   * Creates co group function.
   *
   * @param vertexFactory persistent vertex data factory
   */
  public BuildPersistentVertex(
    PersistentVertexFactory<V, E, PV> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void coGroup(Iterable<Tuple2<V, Set<E>>> iterable,
    Iterable<Tuple2<GradoopId, Set<E>>> iterable1,
    Collector<PersistentVertex<E>> collector) throws Exception {
    V vertex = null;
    Set<E> outgoingEdgeData = null;
    Set<E> incomingEdgeData = null;
    for (Tuple2<V, Set<E>> left : iterable) {
      vertex = left.f0;
      outgoingEdgeData = left.f1;
    }
    for (Tuple2<GradoopId, Set<E>> right : iterable1) {
      incomingEdgeData = right.f1;
    }
    assert vertex != null;
    collector.collect(vertexFactory
      .createVertex(vertex, outgoingEdgeData, incomingEdgeData));
  }
}
