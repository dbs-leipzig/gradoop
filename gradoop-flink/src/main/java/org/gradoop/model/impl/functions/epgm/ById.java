package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;

public class ById<EL extends EPGMElement> implements FilterFunction<EL> {
  private final GradoopId id;

  public ById(GradoopId graphID) {
    this.id = graphID;
  }

  @Override
  public boolean filter(EL graph) throws Exception {
    return graph.getId().equals(id);
  }
}
