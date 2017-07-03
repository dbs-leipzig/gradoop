package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Generates a logical graph
 */
public interface GraphGenerator extends Operator {

  /**
   * generates the graph
   * @return graph collection
   */
  LogicalGraph execute();
}
