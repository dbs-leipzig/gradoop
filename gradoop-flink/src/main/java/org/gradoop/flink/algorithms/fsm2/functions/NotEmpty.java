package org.gradoop.flink.algorithms.fsm2.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;


public class NotEmpty implements FilterFunction<GraphTransaction> {

  @Override
  public boolean filter(GraphTransaction graphTransaction) throws Exception {
    return !graphTransaction.getEdges().isEmpty();
  }
}
