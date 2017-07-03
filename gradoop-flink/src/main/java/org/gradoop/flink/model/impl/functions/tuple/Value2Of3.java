
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => f2
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
public class Value2Of3<T0, T1, T2>
  implements MapFunction<Tuple3<T0, T1, T2>, T2>,
  KeySelector<Tuple3<T0, T1, T2>, T2> {

  @Override
  public T2 map(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f2;
  }

  @Override
  public T2 getKey(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f2;
  }
}
