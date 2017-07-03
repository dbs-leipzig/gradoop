
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
