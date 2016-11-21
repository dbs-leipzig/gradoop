package org.gradoop.flink.algorithms.fsm2.gspan;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;

/**
 * Created by peet on 21.11.16.
 */
public class PatternGrowth
  implements MapFunction<GraphEmbeddingPair, GraphEmbeddingPair> {
  @Override
  public GraphEmbeddingPair map(GraphEmbeddingPair graphEmbeddingPair) throws Exception {

    graphEmbeddingPair.setCodeEmbeddings(Maps.newHashMap());

    return graphEmbeddingPair;
  }
}
