package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.impl.GraphCollection;

/**
 * Generates a graph collection
 */
public interface GraphCollectionGenerator extends Operator {

  /**
   * generates the graph collection
   * @return graph collection
   */
  GraphCollection execute();
}
