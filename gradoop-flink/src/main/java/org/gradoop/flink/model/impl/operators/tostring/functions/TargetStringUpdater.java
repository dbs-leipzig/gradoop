package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * join functions to update the target vertex string representation of an
 * edge string representation
 */
public class TargetStringUpdater implements
  JoinFunction<EdgeString, VertexString, EdgeString> {

  @Override
  public EdgeString join(
    EdgeString edgeString, VertexString targetLabel) throws Exception {

    edgeString.setTargetLabel(targetLabel.getLabel());

    return edgeString;
  }
}
