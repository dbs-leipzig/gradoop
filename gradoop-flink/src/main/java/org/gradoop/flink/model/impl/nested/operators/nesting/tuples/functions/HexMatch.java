package org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * Third projection of the exaplet
 */
@FunctionAnnotation.ForwardedFields("f2->*")
public class HexMatch implements MapFunction<Hexaplet, GradoopId>, KeySelector<Hexaplet, GradoopId> {
  @Override
  public GradoopId map(Hexaplet triple) throws Exception {
    return triple.f2;
  }

  @Override
  public GradoopId getKey(Hexaplet triple) throws Exception {
    return triple.f2;
  }
}
