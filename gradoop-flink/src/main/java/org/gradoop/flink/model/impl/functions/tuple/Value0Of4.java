
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (f0,f1,f2) => f0
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 * @param <T3> f3 type
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class Value0Of4<T0, T1, T2, T3>
  implements
  MapFunction<Tuple4<T0, T1, T2, T3>, T0>,
  KeySelector<Tuple4<T0, T1, T2, T3>, T0> {

  @Override
  public T0 map(Tuple4<T0, T1, T2, T3> quadruple) throws Exception {
    return quadruple.f0;
  }

  @Override
  public T0 getKey(Tuple4<T0, T1, T2, T3> quadruple) throws Exception {
    return quadruple.f0;
  }
}
