
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Join a tuple of an element id and a graph id with a dictionary mapping each
 * graph to a new graph. Result is a tuple of the id of the element and the id
 * of the new graph.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinTuplesWithNewGraphs
  implements JoinFunction<
  Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>> {


  /**
   * Reduce object instantiations
   */
  private Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = right.f1;
    return reuseTuple;
  }
}
