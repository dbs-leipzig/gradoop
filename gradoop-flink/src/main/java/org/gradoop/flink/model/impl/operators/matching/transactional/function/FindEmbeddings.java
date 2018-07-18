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
        GradoopIdSet.fromExisting(embedding.getVertexMapping()),
        GradoopIdSet.fromExisting(embedding.getEdgeMapping())));
    }
  }
}
