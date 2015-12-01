package org.gradoop.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * True, if an element is contained in any of a set of given graphs.
 *
 * @param <GE> element type
 */
public class InAnyGraph<GE extends EPGMGraphElement>
  implements FilterFunction<GE> {

  /**
   * graph ids
   */
  private final GradoopIdSet graphIds;

  /**
   * constructor
   * @param graphIds graph ids
   */
  public InAnyGraph(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().containsAny(this.graphIds);
  }
}
