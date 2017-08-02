package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class ByTargetId<E extends Edge> implements FilterFunction<E> {

  private final GradoopId targetId;

  public ByTargetId(GradoopId targetId) {
    this.targetId = targetId;
  }

  @Override
  public boolean filter(E e) throws Exception {
    return e.getTargetId().equals(targetId);
  }
}
