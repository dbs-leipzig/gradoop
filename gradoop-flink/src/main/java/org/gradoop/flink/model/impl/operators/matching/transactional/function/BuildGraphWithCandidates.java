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
    for (Tuple2<GradoopId, IdWithCandidates<GradoopId>> vertexCandidates : vertices) {
      if (graph == null) {
        graph = new GraphWithCandidates(vertexCandidates.f0);
        graph.setGraphId(vertexCandidates.f0);
      }
      graph.getVertexCandidates().add(vertexCandidates.f1);
    }

    for (Tuple2<GradoopId, TripleWithCandidates<GradoopId>> edgeCandidates : edges) {
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
