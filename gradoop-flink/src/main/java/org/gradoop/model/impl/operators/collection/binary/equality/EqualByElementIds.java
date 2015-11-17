package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.operators.BinaryGraphToBooleanOperator;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByElementIds implements BinaryGraphToBooleanOperator {
  @Override
  public DataSet<Boolean> execute(LogicalGraph firstGraph,
    LogicalGraph secondGraph) {
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
