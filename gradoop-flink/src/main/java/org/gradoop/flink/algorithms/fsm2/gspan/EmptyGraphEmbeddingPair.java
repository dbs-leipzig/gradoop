package org.gradoop.flink.algorithms.fsm2.gspan;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;

public class EmptyGraphEmbeddingPair implements MapFunction<Boolean, GraphEmbeddingPair> {
  @Override
  public GraphEmbeddingPair map(Boolean aBoolean) throws Exception {
    return new GraphEmbeddingPair();
  }
}
