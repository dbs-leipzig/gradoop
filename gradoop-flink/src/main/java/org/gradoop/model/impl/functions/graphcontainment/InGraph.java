package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * True, if an element is contained in a give graph.
 *
 * @param <EL> element type
 */
public class InGraph<EL extends EPGMGraphElement>
  implements FilterFunction<EL> {

  /**
   * graph id
   */
  private final GradoopId graphId;

  /**
   * constructor
   * @param graphId graph id
   */
  public InGraph(GradoopId graphId) {
    this.graphId = graphId;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getGraphIds().contains(this.graphId);
  }
}
