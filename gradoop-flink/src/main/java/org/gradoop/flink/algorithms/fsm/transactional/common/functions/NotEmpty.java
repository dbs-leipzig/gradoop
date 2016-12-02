package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.representation.transactional.GraphTransaction;


public class NotEmpty implements FilterFunction<GraphTransaction> {

  @Override
  public boolean filter(GraphTransaction graphTransaction) throws Exception {
    return !graphTransaction.getEdges().isEmpty();
  }
}
