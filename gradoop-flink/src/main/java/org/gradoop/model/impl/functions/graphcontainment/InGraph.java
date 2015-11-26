package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 26.11.15.
 */
public class InGraph<EL extends EPGMGraphElement>
  implements FilterFunction<EL> {

  private final GradoopId graphId;

  public InGraph(GradoopId graphId) {
    this.graphId = graphId;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getGraphIds().contains(this.graphId);
  }
}
