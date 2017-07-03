package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * edge -> (targetId, firstGraphId)
 * @param <E> edge type
 */
public class TargetIdBtgId<E extends EPGMEdge> implements
  MapFunction<E, Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> map(E e) throws Exception {
    return new Tuple2<>(e.getTargetId(), e.getGraphIds().iterator().next());
  }
}
