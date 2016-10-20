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
