package org.gradoop.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * A pair of {@link org.gradoop.model.impl.id.GradoopId} representing an edge
 * identifier and a target vertex identifier.
 */
public class IdPair extends Tuple2<GradoopId, GradoopId> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId first) {
    f0 = first;
  }

  public GradoopId getTargetId() {
    return f1;
  }

  public void setTargetId(GradoopId second) {
    f1 = second;
  }
}
