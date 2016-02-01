package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

public class TargetIdBtgId<E extends EPGMEdge> implements
  MapFunction<E, Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> map(E e) throws Exception {
    return new Tuple2<>(e.getTargetId(), e.getGraphIds().iterator().next());
  }
}
