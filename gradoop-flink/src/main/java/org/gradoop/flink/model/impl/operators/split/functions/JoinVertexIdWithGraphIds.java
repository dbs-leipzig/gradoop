
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Join the new GradoopIds, representing the new graphs, with the vertices by
 * adding them to the vertices graph sets
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class JoinVertexIdWithGraphIds
  implements JoinFunction<Tuple2<GradoopId, PropertyValue>,
  Tuple2<PropertyValue, GradoopId>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, PropertyValue> vertexSplitKey,
      Tuple2<PropertyValue, GradoopId> splitKeyGradoopId) {
    reuseTuple.setFields(vertexSplitKey.f0, splitKeyGradoopId.f1);
    return reuseTuple;
  }
}
