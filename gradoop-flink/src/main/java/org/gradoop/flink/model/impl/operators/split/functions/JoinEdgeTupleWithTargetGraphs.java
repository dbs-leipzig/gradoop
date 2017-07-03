
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Join edge tuples with the graph sets of their targets
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinEdgeTupleWithTargetGraphs<E extends Edge>
  implements JoinFunction
  <Tuple2<E, GradoopIdList>, Tuple2<GradoopId, GradoopIdList>,
    Tuple3<E, GradoopIdList, GradoopIdList>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<E, GradoopIdList, GradoopIdList> reuseTuple =
    new Tuple3<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<E, GradoopIdList, GradoopIdList> join(
    Tuple2<E, GradoopIdList> left,
    Tuple2<GradoopId, GradoopIdList> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = right.f1;
    return reuseTuple;
  }
}
