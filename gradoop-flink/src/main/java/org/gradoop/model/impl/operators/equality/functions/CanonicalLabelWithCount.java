package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public class CanonicalLabelWithCount implements
  MapFunction<DataLabel, Tuple2<String, Long>> {
  @Override
  public Tuple2<String, Long> map(DataLabel dataLabel) throws Exception {
    return new Tuple2<>(dataLabel.getLabel(), 1L);
  }
}
