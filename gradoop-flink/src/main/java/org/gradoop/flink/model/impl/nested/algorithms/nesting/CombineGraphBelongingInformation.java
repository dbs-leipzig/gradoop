package org.gradoop.flink.model.impl.nested.algorithms.nesting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.tuples.Quad;

/**
 * Created by vasistas on 10/03/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0; f1 -> f1; f2 -> f2; f3 -> f3; f4 -> f4")
@FunctionAnnotation.ForwardedFieldsSecond("f0 -> f5")
public class CombineGraphBelongingInformation implements JoinFunction<Quad,Tuple2<GradoopId,
  GradoopId>, Quad>  {

  @Override
  public Quad join(Quad first, Tuple2<GradoopId, GradoopId> second) throws Exception {
    first.f5 = second.f0;
    return first;
  }
}
