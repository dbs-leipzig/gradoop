
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Join edge tuples with the graph sets of their sources
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinEdgeTupleWithSourceGraphs<E extends Edge>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopIdList>,
  Tuple2<E, GradoopIdList>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<E, GradoopIdList> reuseTuple = new Tuple2<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<E, GradoopIdList> join(
    E left, Tuple2<GradoopId, GradoopIdList> right) {
    reuseTuple.f0 = left;
    reuseTuple.f1 = right.f1;
    return reuseTuple;
  }
}
