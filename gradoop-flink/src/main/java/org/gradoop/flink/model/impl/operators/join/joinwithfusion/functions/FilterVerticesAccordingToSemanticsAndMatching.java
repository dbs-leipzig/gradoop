package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;

/**
 * Match the operands' vertices according to the graph patterns
 *
 * Created by vasistas on 16/02/17.
 */
public class FilterVerticesAccordingToSemanticsAndMatching implements
  FlatJoinFunction<Vertex, Tuple2<GradoopId,Vertex>, DisambiguationTupleWithVertexId> {

  /**
   * Reusable stuff
   */
  private final DisambiguationTupleWithVertexId dt;

  /**
   * Default constructor
   */
  public FilterVerticesAccordingToSemanticsAndMatching() {
    dt = new DisambiguationTupleWithVertexId();
  }

  @Override
  public void join(Vertex first, Tuple2<GradoopId, Vertex> second,
    Collector<DisambiguationTupleWithVertexId> out) throws Exception {
    dt.f0 = first;
    if (second == null) {
      dt.f1 = false;
      dt.f2 = GradoopId.NULL_VALUE;
    } else {
      dt.f1 = true;
      dt.f2 = second.f0;
    }
    out.collect(dt);
  }
}
