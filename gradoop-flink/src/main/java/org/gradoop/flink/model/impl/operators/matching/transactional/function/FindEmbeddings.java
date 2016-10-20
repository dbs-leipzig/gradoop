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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.PatternMatchingAlgorithm;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;

import java.util.List;

/**
 * Mapping function that applies a custom pattern matching algorithm to
 * a GraphWithCandidates, representing a graph, its elements and their
 * candidates. Returns the found embeddings.
 */
public class FindEmbeddings
  implements FlatMapFunction<
  GraphWithCandidates,
  Tuple4<GradoopId, GradoopId, GradoopIdSet, GradoopIdSet>> {

  /**
   * The pattern matching algorithm.
   */
  private PatternMatchingAlgorithm algo;

  /**
   * The query string, input fo the pattern matching algorithm.
   */
  private String query;

  /**
   * Constructor
   * @param algo custom algorithm
   * @param query query string
   */
  public FindEmbeddings(PatternMatchingAlgorithm algo, String query) {
    this.algo = algo;
    this.query = query;
  }


  @Override
  public void flatMap(GraphWithCandidates graphWithCandidates,
    Collector<Tuple4<GradoopId, GradoopId, GradoopIdSet, GradoopIdSet>> collector) throws
    Exception {
    List<Embedding<GradoopId>> embeddings =
      this.algo.findEmbeddings(graphWithCandidates, this.query);

    for (Embedding<GradoopId> embedding : embeddings) {
      GradoopId newGraphId = GradoopId.get();
      collector.collect(new Tuple4<>(newGraphId,
        graphWithCandidates.f0,
        GradoopIdSet.fromExisting(embedding.getVertexMappings()),
        GradoopIdSet.fromExisting(embedding.getEdgeMappings())));
    }
  }
}
