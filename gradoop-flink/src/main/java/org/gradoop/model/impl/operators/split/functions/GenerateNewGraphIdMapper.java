package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Maps the split value to a Tuple2 of the value and an unique GradoopId
 */
public class GenerateNewGraphIdMapper implements
  MapFunction<Tuple1<PropertyValue>, Tuple2<PropertyValue, GradoopId>> {
  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<PropertyValue, GradoopId> map(Tuple1<PropertyValue> splitKey) {
    return new Tuple2<>(splitKey.f0, GradoopId.get());
  }
}

