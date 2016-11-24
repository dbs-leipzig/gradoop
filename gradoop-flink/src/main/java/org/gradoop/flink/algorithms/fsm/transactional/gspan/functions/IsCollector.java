package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;

public class IsCollector implements FilterFunction<GraphEmbeddingsPair> {

  @Override
  public boolean filter(GraphEmbeddingsPair graphEmbeddingsPair) throws Exception {
    return graphEmbeddingsPair.getAdjacencyList().getRows().isEmpty();
  }

}
