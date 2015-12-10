package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * (f0,f1,f2,f3) => (f1,f2,f3)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 * @param <T2> f2 type
 * @param <T3> f3 type
 */
@FunctionAnnotation.ForwardedFields("f1;f2;f3")
public class Project4To1And2And3<T0, T1, T2, T3>
  implements MapFunction<Tuple4<T0, T1, T2, T3>, Tuple3<T1, T2, T3>> {

  @Override
  public Tuple3<T1, T2, T3> map(Tuple4<T0, T1, T2, T3> tuple4) throws
    Exception {
    return new Tuple3<>(tuple4.f1, tuple4.f2, tuple4.f3);
  }
}
