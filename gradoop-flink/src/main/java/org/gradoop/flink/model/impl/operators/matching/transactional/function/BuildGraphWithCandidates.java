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

package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;

/**
 * Constructs GraphWithCandidates from sets of vertices and edges with their
 * candidates.
 */
public class BuildGraphWithCandidates implements
  CoGroupFunction<Tuple2<GradoopId, IdWithCandidates<GradoopId>>,
    Tuple2<GradoopId, TripleWithCandidates<GradoopId>>, GraphWithCandidates> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, IdWithCandidates<GradoopId>>> vertices,
    Iterable<Tuple2<GradoopId, TripleWithCandidates<GradoopId>>> edges,
    Collector<GraphWithCandidates> collector) throws Exception {

    GraphWithCandidates graph = null;
    for (Tuple2<GradoopId, IdWithCandidates<GradoopId>> vertexCandidates :
      vertices) {
      if (graph == null) {
        graph = new GraphWithCandidates(vertexCandidates.f0);
        graph.setGraphId(vertexCandidates.f0);
      }
      graph.getVertexCandidates().add(vertexCandidates.f1);
    }

    for (Tuple2<GradoopId, TripleWithCandidates<GradoopId>> edgeCandidates :
      edges) {
      if (graph == null) {
        graph = new GraphWithCandidates(edgeCandidates.f0);
        graph.setGraphId(edgeCandidates.f0);
      }
      graph.getEdgeCandidates().add(edgeCandidates.f1);
    }

    if (graph != null) {
      collector.collect(graph);
    }
  }
}
