package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class ExpandResult
  implements FlatMapFunction<GraphEmbeddingsPair, TraversalCode<String>> {

  @Override
  public void flatMap(GraphEmbeddingsPair graphEmbeddingsPair,
    Collector<TraversalCode<String>> collector) throws Exception {

    for (TraversalCode<String> code : graphEmbeddingsPair.getCodeEmbeddings().keySet()) {
      System.out.println(code);

      collector.collect(code);
    }
  }
}
