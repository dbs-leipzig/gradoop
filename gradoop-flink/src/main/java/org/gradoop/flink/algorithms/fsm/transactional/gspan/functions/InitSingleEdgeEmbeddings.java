package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class InitSingleEdgeEmbeddings
  implements MapFunction<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>, GraphEmbeddingsPair> {

  private final GSpanKernel gSpan;

  public InitSingleEdgeEmbeddings(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public GraphEmbeddingsPair map(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph) throws Exception {

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      gSpan.getSingleEdgePatternEmbeddings(graph);

    return new GraphEmbeddingsPair(graph, codeEmbeddings);
  }

}
