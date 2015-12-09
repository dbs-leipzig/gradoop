package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Adds new graph id's to the initial vertex set
 *
 * @param <V> EPGM vertex type
 */
public  class AddNewGraphsToVertexJoin<V extends EPGMVertex> implements
  JoinFunction<V, Tuple2<GradoopId, List<GradoopId>>, V> {
  /**
   * {@inheritDoc}
   */
  @Override
  public V join(V v,
    Tuple2<GradoopId, List<GradoopId>> tuple2) {
    v.getGraphIds().addAll(tuple2.f1);
    return v;
  }

}
