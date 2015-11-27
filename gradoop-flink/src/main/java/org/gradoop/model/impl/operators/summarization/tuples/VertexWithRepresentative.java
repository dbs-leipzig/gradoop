package org.gradoop.model.impl.operators.summarization.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Representation of a vertex id and its corresponding vertex group
 * representative.
 *
 * f0: vertex id
 * f1: group representative vertex id
 */
public class VertexWithRepresentative extends Tuple2<GradoopId, GradoopId> {

  public GradoopId getVertexId() {
    return f0;
  }

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getGroupRepresentativeVertexId() {
    return f1;
  }

  public void setGroupRepresentativeVertexId(
    GradoopId groupRepresentativeVertexId) {
    f1 = groupRepresentativeVertexId;
  }
}
