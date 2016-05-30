package org.gradoop.model.impl.operators.matching.isomorphism.naive.functions;

import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Represents a vertex during a traversal.
 */
public class VertexStep extends Tuple1<GradoopId> {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }
}
