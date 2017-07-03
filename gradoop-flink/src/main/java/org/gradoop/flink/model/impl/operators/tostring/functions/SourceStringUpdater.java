package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * join functions to update the source vertex string representation of an
 * edge string representation
 */
public class SourceStringUpdater
  implements JoinFunction<EdgeString, VertexString, EdgeString> {

  @Override
  public EdgeString join(
    EdgeString edgeString, VertexString sourceLabel) throws Exception {

    edgeString.setSourceLabel(sourceLabel.getLabel());

    return edgeString;
  }
}
