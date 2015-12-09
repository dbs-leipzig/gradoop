package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Join the new GradoopIds, representing the new graphs, with the vertices by
 * adding them to the vertices graph sets
 */
public class JoinVertexIdWithGraphIds implements
  JoinFunction
    <Tuple2<GradoopId, PropertyValue>, Tuple2<PropertyValue, GradoopId>,
      Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, PropertyValue> vertexSplitKey,
      Tuple2<PropertyValue, GradoopId> splitKeyGradoopId) {
    return new Tuple2<>(vertexSplitKey.f0, splitKeyGradoopId.f1);
  }
}
