package org.gradoop.flink.algorithms.fsm2.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm2.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class InitSingleEdgeEmbeddings
  implements MapFunction<AdjacencyList<LabelPair>, GraphEmbeddingPair> {

  @Override
  public GraphEmbeddingPair map(AdjacencyList<LabelPair> graph) throws Exception {

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      GSpan.getSingleEdgeCodeEmbeddings(graph);

    return new GraphEmbeddingPair(graph, codeEmbeddings);
  }

}
