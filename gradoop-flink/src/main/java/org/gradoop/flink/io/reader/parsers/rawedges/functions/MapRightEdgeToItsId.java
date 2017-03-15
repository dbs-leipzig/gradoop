package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Maps each right edge to its id
 */
@FunctionAnnotation.ForwardedFields("f0 -> f0")
public class MapRightEdgeToItsId implements MapFunction<Tuple2<GradoopId, Edge>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reusable element
   */
  private Tuple2<GradoopId, GradoopId> gid;

  /**
   * Default constructor
   */
  public MapRightEdgeToItsId() {
    gid = new Tuple2<>();
  }

  @Override
  public Tuple2<GradoopId, GradoopId> map(Tuple2<GradoopId, Edge> value) throws Exception {
    gid.f0 = value.f0;
    gid.f1 = value.f1.getId();
    return gid;
  }
}
