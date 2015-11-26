package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopIdSet;

public class InGraphs<EL extends EPGMGraphElement>
  implements FilterFunction<EL> {

  private final GradoopIdSet graphIds;

  public InGraphs(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getGraphIds().containsAll(this.graphIds);
  }
}
