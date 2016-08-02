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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.storage.api.PersistentVertexFactory;

import java.util.Set;

/**
 * Used to create persistent vertex data from vertex data and
 * outgoing/incoming edge data.
 */
public class BuildPersistentVertex implements CoGroupFunction
  <Tuple2<EPGMVertex, Set<EPGMEdge>>, Tuple2<GradoopId, Set<EPGMEdge>>, PersistentVertex> {

  /**
   * Persistent vertex data factory.
   */
  private final PersistentVertexFactory vertexFactory;

  /**
   * Creates co group function.
   *
   * @param vertexFactory persistent vertex data factory
   */
  public BuildPersistentVertex(PersistentVertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void coGroup(Iterable<Tuple2<EPGMVertex, Set<EPGMEdge>>> iterable,
    Iterable<Tuple2<GradoopId, Set<EPGMEdge>>> iterable1,
    Collector<PersistentVertex> collector) throws Exception {
    EPGMVertex vertex = null;
    Set<EPGMEdge> outgoingEdgeData = null;
    Set<EPGMEdge> incomingEdgeData = null;
    for (Tuple2<EPGMVertex, Set<EPGMEdge>> left : iterable) {
      vertex = left.f0;
      outgoingEdgeData = left.f1;
    }
    for (Tuple2<GradoopId, Set<EPGMEdge>> right : iterable1) {
      incomingEdgeData = right.f1;
    }
    assert vertex != null;
    collector.collect(vertexFactory
      .createVertex(vertex, outgoingEdgeData, incomingEdgeData));
  }
}
