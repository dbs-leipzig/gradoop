package org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Represents an edge that is joined with an embedding to extend it at the weld
 * point.
 *
 * f0: edge id
 * f1: tie point (sourceId/targetId)
 * f2: next id (sourceId/targetId)
 */
public class EdgeStep extends Tuple3<GradoopId, GradoopId, GradoopId> {

  public GradoopId getEdgeId() {
    return f0;
  }

  public void setEdgeId(GradoopId edgeId) {
    f0 = edgeId;
  }

  public GradoopId getTiePoint() {
    return f1;
  }

  public void setTiePointId(GradoopId tiePoint) {
    f1 = tiePoint;
  }

  public GradoopId getNextId() {
    return f2;
  }

  public void setNextId(GradoopId nextId) {
    f2 = nextId;
  }
}
