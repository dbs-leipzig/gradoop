package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 17/02/17.
 */
public class FlatJoinSourceEdgeReference implements
  FlatJoinFunction<Edge, Tuple2<Vertex, GradoopId>, Edge> {

  private final boolean isItSourceDoingNow;

  public FlatJoinSourceEdgeReference(boolean isItSourceDoingNow) {
    this.isItSourceDoingNow = isItSourceDoingNow;
  }

  @Override
  public void join(Edge first, Tuple2<Vertex, GradoopId> second, Collector<Edge> out) throws
    Exception {
    if (second != null && second.f1.equals(GradoopId.NULL_VALUE)) {
      if (isItSourceDoingNow) {
        first.setSourceId(second.f1);
      } else {
        first.setTargetId(second.f1);
      }
    }
    out.collect(first);
  }

}
