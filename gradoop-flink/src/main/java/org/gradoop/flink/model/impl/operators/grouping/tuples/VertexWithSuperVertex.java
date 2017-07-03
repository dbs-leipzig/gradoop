
package org.gradoop.flink.model.impl.operators.grouping.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Representation of a vertex id and its corresponding vertex group
 * representative.
 *
 * f0: vertex id
 * f1: group representative vertex id
 */
public class VertexWithSuperVertex extends Tuple2<GradoopId, GradoopId> {

  public void setVertexId(GradoopId vertexId) {
    f0 = vertexId;
  }

  public GradoopId getSuperVertexId() {
    return f1;
  }

  public void setSuperVertexId(GradoopId superVertexId) {
    f1 = superVertexId;
  }
}
