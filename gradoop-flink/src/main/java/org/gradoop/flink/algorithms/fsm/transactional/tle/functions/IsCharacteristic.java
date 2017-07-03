
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;

/**
 * Filters characteristic subgraphs.
 */
public class IsCharacteristic implements FilterFunction<CCSSubgraph> {

  @Override
  public boolean filter(CCSSubgraph subgraph) throws Exception {
    return subgraph.isInteresting();
  }
}
