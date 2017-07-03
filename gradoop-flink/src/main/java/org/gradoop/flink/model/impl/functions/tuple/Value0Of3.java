
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * (f0,f1,f2) => f0
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class Value0Of3<T0, T1, T2>
  implements
  MapFunction<Tuple3<T0, T1, T2>, T0>, KeySelector<Tuple3<T0, T1, T2>, T0> {

  @Override
  public T0 map(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f0;
  }

  @Override
  public T0 getKey(Tuple3<T0, T1, T2> triple) throws Exception {
    return triple.f0;
  }
}
