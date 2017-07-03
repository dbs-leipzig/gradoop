
package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;

/**
 * (V, E) => true, if E not empty
 */
public class NotEmpty implements FilterFunction<int[]> {

  /**
   * util methods to interpret int-array encoded graphs
   */
  private final GraphUtils graphUtils = new GraphUtilsBase();

  @Override
  public boolean filter(int[] graph) throws Exception {
    return graphUtils.getEdgeCount(graph) > 0;
  }
}
