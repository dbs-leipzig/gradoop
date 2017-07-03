package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Wraps the result of a group-by-id-count into a property value
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1")
public class GroupCountToPropertyValue implements
  MapFunction<Tuple2<GradoopId, Long>, Tuple2<GradoopId, PropertyValue>> {

  @Override
  public Tuple2<GradoopId, PropertyValue> map(
    Tuple2<GradoopId, Long> pair) throws Exception {
    return new Tuple2<>(pair.f0, PropertyValue.create(pair.f1));
  }
}
