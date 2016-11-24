package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class InitSingleEdgeEmbeddings
  implements MapFunction<AdjacencyList<LabelPair>, GraphEmbeddingsPair> {

  private final GSpanKernel gSpan;

  public InitSingleEdgeEmbeddings(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public GraphEmbeddingsPair map(AdjacencyList<LabelPair> graph) throws Exception {

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      gSpan.getSingleEdgePatternEmbeddings(graph);

    return new GraphEmbeddingsPair(graph, codeEmbeddings);
  }

}
