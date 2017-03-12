package org.gradoop.flink.model.impl.nested.operators.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;

/**
 * Associates each exaplet to the nested graph where it appears
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0; f1 -> f1; f2 -> f2; f3 -> f3; f4 -> f4")
@FunctionAnnotation.ForwardedFieldsSecond("f0 -> f5")
public class CombineGraphBelongingInformation implements JoinFunction<Hexaplet, Tuple2<GradoopId,
  GradoopId>, Hexaplet>  {

  @Override
  public Hexaplet join(Hexaplet first, Tuple2<GradoopId, GradoopId> second) throws Exception {
    first.f5 = second.f0;
    return first;
  }
}
