
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Join the edge tuples with the new graph ids.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f2->f2")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f3")
public class EdgesWithNewGraphsTuple
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>,
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> reuseTuple =
    new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> join(
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = left.f2;
    reuseTuple.f3 = right.f1;
    return reuseTuple;
  }
}
