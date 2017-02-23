package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

/**
 * Created by vasistas on 23/02/17.
 */
public class FlatJoinSourceEdgeReference3 implements JoinFunction<Edge, VertexIdToEdgeId, Edge>{

  /**
   * Checking if the stuff is actually updating the sources.
   * Otherwise, it updates the targets
   */
  private final boolean isItSourceDoingNow;

  /**
   * Default constructor
   * @param b Determines if I'm checking the sources (true) or not (false)
   */
  public FlatJoinSourceEdgeReference3(boolean b) {
    isItSourceDoingNow = b;
  }

  @Override
  public Edge join(Edge first, VertexIdToEdgeId second) throws Exception {
    if (second != null && !(second.f1.equals(GradoopId.NULL_VALUE))) {
      if (isItSourceDoingNow) {
        first.setSourceId(second.f1);
      } else {
        first.setTargetId(second.f1);
      }
    }
    return first;
  }
}
