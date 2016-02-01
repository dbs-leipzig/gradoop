package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

public class BtgId implements MapFunction
  <Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public Tuple2<GradoopId, GradoopIdSet> map(
    Tuple2<GradoopId, GradoopIdSet> pair) throws
    Exception {
    return new Tuple2<>(GradoopId.get(), pair.f1);
  }
}
