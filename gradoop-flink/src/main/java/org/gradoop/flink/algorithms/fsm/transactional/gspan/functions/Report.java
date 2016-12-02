package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

public class Report
  implements FlatMapFunction<GraphEmbeddingsPair, WithCount<TraversalCode<String>>> {

  @Override
  public void flatMap(GraphEmbeddingsPair graphEmbeddings,
    Collector<WithCount<TraversalCode<String>>> collector) throws Exception {

    if (!graphEmbeddings.getAdjacencyList().getOutgoingRows().isEmpty()) {
      for (TraversalCode<String> code : graphEmbeddings.getCodeEmbeddings().keySet()) {
        collector.collect(new WithCount<>(code));
      }
    }
  }
}
