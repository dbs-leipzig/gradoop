package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

/**
 * Common functionality of directed and undirected gSpan implementations.
 */
public interface GSpanKernel {

  /**
   * Finds all 1-edge patterns and their embeddings in a given graph.
   *
   * @param graph graph
   * @return pattern -> embeddings
   */
  Map<TraversalCode<String>,Collection<TraversalEmbedding>> getSingleEdgePatternEmbeddings(
    AdjacencyList<LabelPair> graph);

  /**
   * Grows children of supported frequent patterns in a graph.
   *
   * @param graphEmbeddingsPair (graph, pattern -> embeddings)
   * @param frequentPatterns [frequentPattern,..]
   */
  void growChildren(GraphEmbeddingsPair graphEmbeddingsPair,
    Collection<TraversalCode<String>> frequentPatterns);

  /**
   * Checks if a pattern in DFS-code representation
   * is minimal according to gSpan lexicographic order.
   *
   * @param pattern
   * @return
   */
  boolean isMinimal(TraversalCode<String> pattern);

}
