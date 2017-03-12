package org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * Fith projection of the Exaplet
 */
@FunctionAnnotation.ForwardedFields("f4->*")
public class Hex4
  implements MapFunction<Hexaplet, GradoopId>, KeySelector<Hexaplet, GradoopId> {

  @Override
  public GradoopId map(Hexaplet triple) throws Exception {
    return triple.f4;
  }

  @Override
  public GradoopId getKey(Hexaplet triple) throws Exception {
    return triple.f4;
  }
}
