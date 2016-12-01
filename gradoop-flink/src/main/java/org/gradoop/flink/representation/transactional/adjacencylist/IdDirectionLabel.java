package org.gradoop.flink.representation.transactional.adjacencylist;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

public class IdDirectionLabel extends Tuple3<GradoopId, Boolean, String> {

  public IdDirectionLabel(GradoopId edgeId, boolean outgoing, String label) {
    super(edgeId, outgoing, label);
  }

  public GradoopId getId() {
    return f0;
  }

  public Boolean isOutgoing() {
    return f1;
  }
}
