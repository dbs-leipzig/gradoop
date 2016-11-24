package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public interface GSpan {
  boolean isMinimal(TraversalCode<String> code);

  void growChildren(GraphEmbeddingPair graphEmbeddingPair,
    Collection<TraversalCode<String>> frequentSubgraphs);

  Map<TraversalCode<String>,Collection<TraversalEmbedding>> getSingleEdgeCodeEmbeddings(
    AdjacencyList<LabelPair> graph);
}
