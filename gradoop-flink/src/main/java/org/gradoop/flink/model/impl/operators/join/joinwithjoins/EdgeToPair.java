package org.gradoop.flink.model.impl.operators.join.joinwithjoins;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Created by vasistas on 16/02/17.
 */
public class EdgeToPair implements MapFunction<Edge,
  Tuple2<GradoopId,GradoopId>> {
  @Override
  public Tuple2<GradoopId, GradoopId> map(Edge value) throws Exception {
    return new Tuple2<>(value.getSourceId(),value.getTargetId());
  }
}
