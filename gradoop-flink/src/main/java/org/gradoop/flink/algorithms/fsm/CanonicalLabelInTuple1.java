package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSubgraphDecoder;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;


public class CanonicalLabelInTuple1 implements
  MapFunction<GraphTransaction,Tuple1<String>> {
  @Override
  public Tuple1<String> map(GraphTransaction graphTransaction) throws
    Exception {
    return new Tuple1<>(graphTransaction.getGraphHead().getPropertyValue
      (TFSMSubgraphDecoder.CANONICAL_LABEL_KEY).getString());
  }
}
