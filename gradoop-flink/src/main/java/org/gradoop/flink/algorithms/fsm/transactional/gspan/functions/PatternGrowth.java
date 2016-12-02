package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.common.Constants;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

import java.util.Collection;

public class PatternGrowth
  extends RichMapFunction<GraphEmbeddingsPair, GraphEmbeddingsPair> {

  private Collection<TraversalCode<String>> frequentSubgraphs;
  private final GSpanKernel gSpan;

  public PatternGrowth(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentSubgraphs = getRuntimeContext().getBroadcastVariable(Constants.FREQUENT_SUBGRAPHS);
  }

  @Override
  public GraphEmbeddingsPair map(GraphEmbeddingsPair graphEmbeddingsPair) throws Exception {

    if (graphEmbeddingsPair.getAdjacencyList().getOutgoingRows().isEmpty()) {
      for (TraversalCode<String> code : frequentSubgraphs) {
        graphEmbeddingsPair.getCodeEmbeddings().put(code, null);
      }
    } else {
      gSpan.growChildren(graphEmbeddingsPair, frequentSubgraphs);
    }

    return graphEmbeddingsPair;
  }
}
