package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => (f0,f1)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
public class Project3To0And1<T0, T1, T2>
  implements MapFunction<Tuple3<T0, T1, T2>, Tuple2<T0, T1>> {

  @Override
  public Tuple2<T0, T1> map(Tuple3<T0, T1, T2> triple) throws
    Exception {
    return new Tuple2<>(triple.f0, triple.f1);
  }
}
