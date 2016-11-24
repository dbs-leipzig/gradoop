package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;

public class EmptyGraphEmbeddingPair implements MapFunction<Boolean, GraphEmbeddingsPair> {
  @Override
  public GraphEmbeddingsPair map(Boolean aBoolean) throws Exception {
    return new GraphEmbeddingsPair();
  }
}
