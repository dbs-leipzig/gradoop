package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by peet on 01.02.16.
 */
public class SwitchPair<A, B>
  implements MapFunction<Tuple2<A, B>, Tuple2<B, A>> {
  @Override
  public Tuple2<B, A> map(Tuple2<A, B> pair) throws Exception {
    return new Tuple2<>(pair.f1, pair.f0);
  }
}
