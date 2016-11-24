package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpan;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class InitSingleEdgeEmbeddings
  implements MapFunction<AdjacencyList<LabelPair>, GraphEmbeddingPair> {

  private final GSpan gSpan;

  public InitSingleEdgeEmbeddings(GSpan gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public GraphEmbeddingPair map(AdjacencyList<LabelPair> graph) throws Exception {

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      gSpan.getSingleEdgeCodeEmbeddings(graph);

    return new GraphEmbeddingPair(graph, codeEmbeddings);
  }

}
