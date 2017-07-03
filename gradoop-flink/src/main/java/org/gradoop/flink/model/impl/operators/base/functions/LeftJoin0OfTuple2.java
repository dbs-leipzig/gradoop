
package org.gradoop.flink.model.impl.operators.base.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Left join, return first value of Tuple2.
 *
 * @param <L> left type
 * @param <R> right type
 */
@FunctionAnnotation.ForwardedFields("*->f0")
public class LeftJoin0OfTuple2<L, R> implements
  JoinFunction<Tuple2<L, GradoopId>, R, L> {

  @Override
  public L join(Tuple2<L, GradoopId> tuple, R r) throws Exception {
    return tuple.f0;
  }
}
