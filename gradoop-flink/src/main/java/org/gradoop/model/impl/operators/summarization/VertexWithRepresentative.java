package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Representation of a vertex id and its corresponding vertex group
 * representative.
 */
public class VertexWithRepresentative extends Tuple2<Long, Long> {

  public Long getVertexId() {
    return f0;
  }

  public void setVertexId(Long vertexId) {
    f0 = vertexId;
  }

  public Long getGroupRepresentativeVertexId() {
    return f1;
  }

  public void setGroupRepresentativeVertexId(Long groupRepresentativeVertexId) {
    f1 = groupRepresentativeVertexId;
  }
}
