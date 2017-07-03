
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * switches source and target id of an edge string representation
 */
public class SwitchSourceTargetIds implements
  MapFunction<EdgeString, EdgeString> {

  @Override
  public EdgeString map(EdgeString edgeString) throws Exception {

    GradoopId sourceId = edgeString.getSourceId();
    edgeString.setSourceId(edgeString.getTargetId());
    edgeString.setTargetId(sourceId);

    return edgeString;
  }
}
