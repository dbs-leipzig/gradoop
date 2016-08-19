package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

public class TransactionFromTuple<T> implements
  MapFunction<Tuple2<GraphTransaction, T>, GraphTransaction> {
  @Override
  public GraphTransaction map(
    Tuple2<GraphTransaction, T> tuple) throws  Exception {
    return tuple.f0;
  }
}
