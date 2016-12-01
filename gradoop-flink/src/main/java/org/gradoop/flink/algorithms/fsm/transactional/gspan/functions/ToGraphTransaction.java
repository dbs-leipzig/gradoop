package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernelBase;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * Created by peet on 21.11.16.
 */
public class ToGraphTransaction implements MapFunction<TraversalCode<String>, GraphTransaction> {
  @Override
  public GraphTransaction map(TraversalCode<String> stringTraversalCode) throws Exception {
    return GSpanKernelBase.createGraphTransaction(stringTraversalCode);
  }
}
