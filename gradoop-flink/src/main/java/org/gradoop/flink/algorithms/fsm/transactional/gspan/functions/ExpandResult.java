package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class ExpandResult
  implements FlatMapFunction<GraphEmbeddingPair, TraversalCode<String>> {

  @Override
  public void flatMap(GraphEmbeddingPair graphEmbeddingPair,
    Collector<TraversalCode<String>> collector) throws Exception {

    for (TraversalCode<String> code : graphEmbeddingPair.getCodeEmbeddings().keySet()) {
      System.out.println(code);
      collector.collect(code);
    }
  }
}
