package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Switches the fields of a Flink pair
 * @param <A> type of field 0
 * @param <B> type of field 1
 */
@FunctionAnnotation.ForwardedFields("f0->f1;f1->f0")
public class SwitchPair<A, B>
  implements MapFunction<Tuple2<A, B>, Tuple2<B, A>> {

  @Override
  public Tuple2<B, A> map(Tuple2<A, B> pair) throws Exception {
    return new Tuple2<>(pair.f1, pair.f0);
  }
}
