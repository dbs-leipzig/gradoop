package org.gradoop.flink.representation.transactional.adjacencylist;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;


public class IdLabel extends Tuple2<GradoopId, String> {

  public IdLabel(GradoopId id, String label) {
    super(id, label);
  }

  public GradoopId getId() {
    return f0;
  }
}
