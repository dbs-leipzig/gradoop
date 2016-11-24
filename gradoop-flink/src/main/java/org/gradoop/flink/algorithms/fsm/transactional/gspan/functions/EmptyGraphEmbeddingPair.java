package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;

public class EmptyGraphEmbeddingPair implements MapFunction<Boolean, GraphEmbeddingPair> {
  @Override
  public GraphEmbeddingPair map(Boolean aBoolean) throws Exception {
    return new GraphEmbeddingPair();
  }
}
