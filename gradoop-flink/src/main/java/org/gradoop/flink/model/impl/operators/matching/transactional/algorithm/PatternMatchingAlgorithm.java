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
package org.gradoop.flink.model.impl.operators.matching.transactional.algorithm;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.transactional.tuples.GraphWithCandidates;

import java.io.Serializable;
import java.util.List;

/**
 * Interface of a custom pattern matching algorithm.
 */
public interface PatternMatchingAlgorithm extends Serializable {
  /**
   * Returns true, if the graph contains the pattern.
   * This should be much faster than findEmbeddings, because early aborting
   * is possible.
   *
   * @param graph a graph with its vertices and edges as well as their
   *              candidates
   * @param query a query string
   * @return true, if the graph contains an instance of the pattern
   */
  Boolean hasEmbedding(GraphWithCandidates graph, String query);

  /**
   * Finds all embeddings of a pattern in a given graph and returns them.
   *
   * @param graph a graph with its vertices and edges as well as their
   *              candidates
   * @param query a query string
   * @return all possible embeddings of the pattern
   */
  List<Embedding<GradoopId>> findEmbeddings(GraphWithCandidates graph, String query);

}
