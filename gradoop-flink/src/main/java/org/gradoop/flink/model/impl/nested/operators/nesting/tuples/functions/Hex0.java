package org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * First projection of the Exaplet
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class Hex0
  implements MapFunction<Hexaplet, GradoopId>, KeySelector<Hexaplet, GradoopId> {

  @Override
  public GradoopId map(Hexaplet triple) throws Exception {
    return triple.f0;
  }

  @Override
  public GradoopId getKey(Hexaplet triple) throws Exception {
    return triple.f0;
  }
}
