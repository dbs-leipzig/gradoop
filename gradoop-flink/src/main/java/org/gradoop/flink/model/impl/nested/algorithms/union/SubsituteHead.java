package org.gradoop.flink.model.impl.nested.algorithms.union;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/03/17.
 */
public class SubsituteHead implements MapFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>> {
  private final GradoopId x;
  public SubsituteHead(GradoopId id) {
    this.x = id;
  }
  @Override
  public Tuple2<GradoopId, GradoopId> map(Tuple2<GradoopId, GradoopId> value) throws Exception {
    value.f0 = x;
    return value;
  }
}
