package org.gradoop.flink.algorithms.fsm2.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class Report
  implements FlatMapFunction<GraphEmbeddingPair, WithCount<TraversalCode<String>>> {

  @Override
  public void flatMap(GraphEmbeddingPair graphEmbeddings, 
    Collector<WithCount<TraversalCode<String>>> collector) throws Exception {

    if (!graphEmbeddings.getAdjacencyLists().getRows().isEmpty()) {
      for (TraversalCode<String> code : graphEmbeddings.getCodeEmbeddings().keySet()) {
        collector.collect(new WithCount<>(code));
      }
    }
  }
}
