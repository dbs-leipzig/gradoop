package org.gradoop.flink.model.impl.nested.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Created by vasistas on 10/03/17.
 */
public class Tuple2Comparator implements
  JoinFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>, Boolean> {
  @Override
  public Boolean join(Tuple2<GradoopId, GradoopId> first,
    Tuple2<GradoopId, GradoopId> second) throws Exception {
    return (first == null || second == null) ? false : first.equals(second);
  }
}
