package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;

public class IsCollector implements FilterFunction<GraphEmbeddingPair> {

  @Override
  public boolean filter(GraphEmbeddingPair graphEmbeddingPair) throws Exception {
    return graphEmbeddingPair.getAdjacencyList().getRows().isEmpty();
  }

}
