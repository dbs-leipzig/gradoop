
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Join an edge tuple with a tuple containing the target vertex id of this edge
 * and its new graphs.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinWithTargetGraphIdSet
  implements JoinFunction<
  Tuple4<GradoopId, GradoopIdList, GradoopId, GradoopIdList>,
  Tuple2<GradoopId, GradoopIdList>,
  Tuple4<GradoopId, GradoopIdList, GradoopIdList, GradoopIdList>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopIdList, GradoopIdList, GradoopIdList> reuseTuple
    = new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopIdList, GradoopIdList, GradoopIdList> join(
    Tuple4<GradoopId, GradoopIdList, GradoopId, GradoopIdList> edge,
    Tuple2<GradoopId, GradoopIdList> vertex) throws
    Exception {
    reuseTuple.f0 = edge.f0;
    reuseTuple.f1 = edge.f1;
    reuseTuple.f2 = vertex.f1;
    reuseTuple.f3 = edge.f3;
    return reuseTuple;
  }
}
