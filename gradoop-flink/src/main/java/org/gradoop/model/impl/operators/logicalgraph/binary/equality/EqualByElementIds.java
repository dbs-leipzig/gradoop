package org.gradoop.model.impl.operators.logicalgraph.binary.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByElementIds implements
  BinaryGraphToValueOperator<EPGMVertex, EPGMEdge, EPGMGraphHead, Boolean> {
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
