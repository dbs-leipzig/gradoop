
package org.gradoop.flink.model.impl.functions.tuple;

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
@FunctionAnnotation.ForwardedFields("f1->f0;f2->f1;f3->f2")
public class Project4To1And2And3<T0, T1, T2, T3>
  implements MapFunction<Tuple4<T0, T1, T2, T3>, Tuple3<T1, T2, T3>> {

  /**
   * Reduce instantiations
   */
  private final Tuple3<T1, T2, T3> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<T1, T2, T3> map(Tuple4<T0, T1, T2, T3> quad) throws Exception {
    reuseTuple.setFields(quad.f1, quad.f2, quad.f3);
    return reuseTuple;
  }
}
