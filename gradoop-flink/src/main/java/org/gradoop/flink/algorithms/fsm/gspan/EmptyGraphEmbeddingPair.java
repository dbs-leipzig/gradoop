package org.gradoop.flink.algorithms.fsm.gspan;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.tuples.GraphEmbeddingPair;

public class EmptyGraphEmbeddingPair implements MapFunction<Boolean, GraphEmbeddingPair> {
  @Override
  public GraphEmbeddingPair map(Boolean aBoolean) throws Exception {
    return new GraphEmbeddingPair();
  }
}
